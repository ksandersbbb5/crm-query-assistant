import os
import json
import re
import traceback
from collections import Counter, defaultdict
from http.server import BaseHTTPRequestHandler
from urllib.parse import quote as urlquote

import openai
from pyairtable import Table
import pymssql
import requests
from requests.exceptions import HTTPError

API_VERSION = "2025-08-29-airtable-paging-v12"

# =============================
# Environment Variables
# =============================
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

AIRTABLE_API_KEY    = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID    = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME")

AZURE_SQL_SERVER   = os.getenv("AZURE_SQL_SERVER")
AZURE_SQL_DB       = os.getenv("AZURE_SQL_DB")
AZURE_SQL_USER     = os.getenv("AZURE_SQL_USER")
AZURE_SQL_PASSWORD = os.getenv("AZURE_SQL_PASSWORD")

# Tuning knobs
DISABLE_AIRTABLE_SUMMARY     = (os.getenv("DISABLE_AIRTABLE_SUMMARY", "true").lower() == "true")
AIRTABLE_DEFAULT_LIMIT       = int(os.getenv("AIRTABLE_DEFAULT_LIMIT", "50"))    # fallback when user doesn't specify N
AIRTABLE_MAX_LIMIT           = int(os.getenv("AIRTABLE_MAX_LIMIT", "5000"))      # overall cap for "all" (paging handles it)
AIRTABLE_SCAN_LIMIT          = int(os.getenv("AIRTABLE_SCAN_LIMIT", "2000"))     # used by aggregations
AIRTABLE_PAGE_SIZE_DEFAULT   = int(os.getenv("AIRTABLE_PAGE_SIZE_DEFAULT", "50"))# per-page size (1..100)

openai.api_key = OPENAI_API_KEY

# =============================
# Airtable Adapter + Helpers
# =============================
_airtable = None
if AIRTABLE_API_KEY and AIRTABLE_BASE_ID and AIRTABLE_TABLE_NAME:
    try:
        _airtable = Table(AIRTABLE_API_KEY, AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)
    except Exception:
        _airtable = None

STATE_CODES = {"MA", "ME", "RI", "VT"}
STATE_NAME_TO_CODE = {
    "massachusetts": "MA",
    "maine": "ME",
    "rhode island": "RI",
    "vermont": "VT",
    "ma": "MA", "me": "ME", "ri": "RI", "vt": "VT",
}

def _safe_to_str(val):
    try:
        return val if isinstance(val, str) else ""
    except Exception:
        return ""

def _to_url_list(value):
    urls = []
    if value is None:
        return urls
    items = value if isinstance(value, list) else [value]
    for x in items:
        u = None
        if isinstance(x, dict):
            candidate = x.get("url")
            if isinstance(candidate, str):
                u = candidate
        elif isinstance(x, str):
            u = x
        u = _safe_to_str(u)
        if u and (u[:4].lower() == "http"):
            urls.append(u)
    return urls

def _normalize_photo_fields(fields: dict):
    candidates = ["Photo", "Photos", "Attachment", "Attachments", "Images", "Image"]
    found_urls = []
    for key in candidates:
        if key in fields:
            urls = _to_url_list(fields.get(key))
            fields[key] = urls
            if not found_urls and urls:
                found_urls = urls
    if "Photo" not in fields:
        fields["Photo"] = found_urls
    fields["first_photo_url"] = found_urls[0] if found_urls else None

def parse_state_and_limit(question: str):
    """Find state and limit; supports 'past|last N' and 'all'."""
    # Prefer 'from|in <state>'
    m = re.search(
        r"\b(?:from|in)\s+(massachusetts|maine|rhode island|vermont|ma|me|ri|vt)\b",
        question,
        flags=re.IGNORECASE,
    )
    state = None
    if m:
        state = STATE_NAME_TO_CODE[m.group(1).lower()]

    # Fallback ALL-CAPS
    if not state:
        m2 = re.search(r"\b(MA|ME|RI|VT)\b", question)
        if m2:
            state = m2.group(1).upper()

    # Limit: explicit number
    limit = None
    m3 = re.search(r"\b(past|last|first|top)\s+(\d+)", question, flags=re.IGNORECASE)
    if m3:
        try:
            limit = int(m3.group(2))
        except Exception:
            pass

    # 'all' keyword → we allow more but will page
    if re.search(r"\ball\b", question, flags=re.IGNORECASE):
        limit = AIRTABLE_MAX_LIMIT

    if limit is None:
        limit = AIRTABLE_DEFAULT_LIMIT

    # clamp overall
    limit = max(1, min(AIRTABLE_MAX_LIMIT, limit))
    return state, limit

def _discover_columns():
    try:
        recs = _airtable.all(max_records=1) if _airtable else []
        if recs:
            fields = recs[0].get("fields", {}) or {}
            return set(fields.keys())
    except Exception:
        pass
    return set()

def _build_formula_for_state(state: str):
    """Return a safe formula string that only references existing columns."""
    if not state or state not in STATE_CODES:
        return None
    existing = _discover_columns()
    candidates = ["State", "state", "State Abbrev", "State Code"]
    usable = [f for f in candidates if f in existing]
    if not usable:
        return None
    clauses = [f'UPPER({{{f}}})="{state}"' for f in usable]
    return "OR(" + ",".join(clauses) + ")"

def _airtable_sort_params(sort_list):
    """
    Convert ["-Date of Event","Name"] → params sort[0][field], sort[0][direction], ...
    """
    params = {}
    idx = 0
    for s in sort_list or []:
        direction = "asc"
        field = s
        if isinstance(s, str) and s.startswith("-"):
            direction = "desc"
            field = s[1:]
        params[f"sort[{idx}][field]"] = field
        params[f"sort[{idx}][direction]"] = direction
        idx += 1
    return params

def _airtable_list_records(formula=None, sort=None, page_size=50, offset=None):
    """One Airtable page via REST; returns (records, next_offset)"""
    if not (AIRTABLE_API_KEY and AIRTABLE_BASE_ID and AIRTABLE_TABLE_NAME):
        return [], None
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{urlquote(AIRTABLE_TABLE_NAME)}"
    params = {}
    if formula:
        params["filterByFormula"] = formula
    # clamp page size to Airtable max=100
    ps = max(1, min(100, int(page_size or 50)))
    params["pageSize"] = ps
    if offset:
        params["offset"] = offset
    params.update(_airtable_sort_params(sort or []))
    headers = {"Authorization": f"Bearer {AIRTABLE_API_KEY}"}
    resp = requests.get(url, headers=headers, params=params, timeout=20)
    resp.raise_for_status()
    data = resp.json()
    return data.get("records", []), data.get("offset")

def get_airtable_photos_page(state=None, page_size=50, cursor=None):
    """Return a single page + next cursor; normalize photos."""
    formula = _build_formula_for_state(state)
    sort = ["-Date of Event"]
    try:
        recs, next_cursor = _airtable_list_records(formula=formula, sort=sort, page_size=page_size, offset=cursor)
    except HTTPError as http_err:
        # If formula invalid for any reason, fallback without it
        if hasattr(http_err.response, "status_code") and http_err.response.status_code == 422:
            recs, next_cursor = _airtable_list_records(formula=None, sort=sort, page_size=page_size, offset=cursor)
        else:
            raise

    rows = []
    for rec in recs:
        fields = rec.get("fields", {}) or {}
        _normalize_photo_fields(fields)
        rows.append(fields)
    return rows, next_cursor

def fetch_airtable_records_for_aggregation(state=None, max_scan=AIRTABLE_SCAN_LIMIT):
    """
    Pull up to max_scan rows using paging for aggregations.
    """
    collected = []
    cursor = None
    page_size = 100  # max per Airtable
    while len(collected) < max_scan:
        remaining = max_scan - len(collected)
        ps = min(page_size, remaining)
        page, cursor = get_airtable_photos_page(state=state, page_size=ps, cursor=cursor)
        if not page:
            break
        collected.extend(page)
        if not cursor:
            break
    return collected

def _extract_employee_name(fields: dict):
    first = fields.get("Employee First Name")
    last  = fields.get("Employee Last Name")

    def _first_string(x):
        if isinstance(x, list) and x:
            return _safe_to_str(x[0]).strip()
        return _safe_to_str(x).strip()

    f = _first_string(first)
    l = _first_string(last)
    if f or l:
        return (l + ", " + f).strip(", ").strip()

    sub = fields.get("Submitted by Employee")
    if isinstance(sub, list) and sub:
        return f"(Employee {sub[0]})"
    return "(Unknown)"

def aggregate_top_employees(state=None, top_n=10):
    rows = fetch_airtable_records_for_aggregation(state=state, max_scan=AIRTABLE_SCAN_LIMIT)
    counter = Counter()
    for r in rows:
        photos = r.get("Photo") or []
        if isinstance(photos, list) and photos:
            name = _extract_employee_name(r)
            counter[name] += 1
    top = counter.most_common(top_n)
    return top, len(rows)

def aggregate_counts_by_state(state=None, top_n=None):
    rows = fetch_airtable_records_for_aggregation(state=state, max_scan=AIRTABLE_SCAN_LIMIT)
    counter = Counter()
    for r in rows:
        photos = r.get("Photo") or []
        if isinstance(photos, list) and photos:
            st = r.get("State") or r.get("state") or ""
            if isinstance(st, list) and st:
                st = st[0]
            st = (st or "Unknown").strip()
            counter[st] += 1
    items = counter.most_common(top_n) if top_n else counter.most_common()
    labels = [k for k, _ in items]
    data   = [v for _, v in items]
    return labels, data, sum(counter.values())

def aggregate_counts_by_employee_last_name(state=None, top_n=None):
    rows = fetch_airtable_records_for_aggregation(state=state, max_scan=AIRTABLE_SCAN_LIMIT)
    counter = Counter()
    for r in rows:
        photos = r.get("Photo") or []
        if isinstance(photos, list) and photos:
            last = r.get("Employee Last Name")
            if isinstance(last, list) and last:
                last = last[0]
            last = (last or "Unknown").strip()
            counter[last] += 1
    items = counter.most_common(top_n) if top_n else counter.most_common()
    labels = [k for k, _ in items]
    data   = [v for _, v in items]
    return labels, data, sum(counter.values())

def aggregate_repeated_events(state=None, min_count=2, top_n=25):
    rows = fetch_airtable_records_for_aggregation(state=state, max_scan=AIRTABLE_SCAN_LIMIT)
    groups = defaultdict(lambda: {"count": 0, "states": Counter(), "dates": []})
    for r in rows:
        name = (r.get("Event Name") or "").strip()
        if not name:
            continue
        groups[name]["count"] += 1
        st = r.get("State") or r.get("state") or ""
        if isinstance(st, list) and st:
            st = st[0]
        st = (st or "").strip()
        if st:
            groups[name]["states"][st] += 1
        date = r.get("Date of Event")
        if isinstance(date, str) and date:
            groups[name]["dates"].append(date)

    items = []
    for name, g in groups.items():
        if g["count"] >= min_count:
            states_sorted = sorted(g["states"].items(), key=lambda x: (-x[1], x[0]))
            items.append({
                "event_name": name,
                "count": g["count"],
                "top_states": [f"{s} ({c})" for s, c in states_sorted[:3]],
                "first_date": min(g["dates"]) if g["dates"] else None,
                "last_date": max(g["dates"]) if g["dates"] else None
            })
    items.sort(key=lambda x: (-x["count"], x["event_name"]))
    return items[:top_n], len(rows)

# =============================
# SQL (unchanged)
# =============================
def run_sql(sql: str):
    conn = None
    try:
        conn = pymssql.connect(
            server=AZURE_SQL_SERVER,
            user=AZURE_SQL_USER,
            password=AZURE_SQL_PASSWORD,
            database=AZURE_SQL_DB,
            login_timeout=5,
            timeout=15,
        )
        cur = conn.cursor(as_dict=True)
        cur.execute(sql)
        rows = cur.fetchall()
        return rows
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass

def config_status():
    return {
        "api_version": API_VERSION,
        "airtable_configured": bool(_airtable),
        "sql_configured": bool(AZURE_SQL_SERVER and AZURE_SQL_DB and AZURE_SQL_USER and AZURE_SQL_PASSWORD),
        "openai_configured": bool(OPENAI_API_KEY),
        "disable_airtable_summary": DISABLE_AIRTABLE_SUMMARY,
        "airtable_default_limit": AIRTABLE_DEFAULT_LIMIT,
        "airtable_max_limit": AIRTABLE_MAX_LIMIT,
        "airtable_scan_limit": AIRTABLE_SCAN_LIMIT,
        "airtable_page_size_default": AIRTABLE_PAGE_SIZE_DEFAULT,
    }

_SQL_BLOCKLIST = re.compile(
    r"(;|--|/\*|\*/|\\x| drop | alter | delete | insert | update | merge | exec | execute | xp_| sp_)",
    flags=re.IGNORECASE,
)

def is_safe_select(sql: str) -> bool:
    if not sql:
        return False
    if _SQL_BLOCKLIST.search(sql):
        return False
    if not re.match(r"^\s*select\s", sql, flags=re.IGNORECASE):
        return False
    return True

# =============================
# LLM (SQL only)
# =============================
def llm_generate_sql(question: str, schema_hint: str = "") -> str:
    if not OPENAI_API_KEY:
        return "SELECT TOP 10 * FROM INFORMATION_SCHEMA.TABLES"
    system = (
        "You translate natural-language CRM questions into a SINGLE, safe, read-only "
        "T-SQL SELECT for Azure SQL Server. Use only tables/views mentioned in the schema hint. "
        "Return ONLY the SQL, with no code fences, no comments, no CTEs, no variables, and no semicolons. "
        "Always limit results with TOP 100."
    )
    user = f"Question: {question}\n\nSchema hint:\n{schema_hint}"
    resp = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=[{"role":"system","content":system},{"role":"user","content":user}],
        temperature=0.0,
        max_tokens=300,
    )
    sql = resp.choices[0].message.content.strip()
    sql = re.sub(r"^```[a-zA-Z]*", "", sql).strip()
    sql = re.sub(r"```$", "", sql).strip()
    sql = re.sub(r"--.*?$", "", sql, flags=re.MULTILINE)
    sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)
    sql = sql.split(";")[0].strip()
    if re.search(r"\bTOP\s+\d+\b", sql, flags=re.IGNORECASE) is None:
        sql = re.sub(r"^\s*select\s", "SELECT TOP 100 ", sql, flags=re.IGNORECASE)
    return sql

def llm_format_answer(question: str, sample_rows: list) -> str:
    if not sample_rows:
        return "No results found for your question."
    if not OPENAI_API_KEY:
        return json.dumps(sample_rows[:5], indent=2)
    resp = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=[
            {"role":"system","content":"Summarize the data into a direct, business-friendly answer (1–2 sentences)."},
            {"role":"user","content":f"Question: {question}\n\nRows:\n{json.dumps(sample_rows[:5], indent=2)}"},
        ],
        temperature=0.2,
        max_tokens=300,
    )
    return resp.choices[0].message.content.strip()

# =============================
# Intent detection (Airtable)
# =============================
def is_employee_most_photos_intent(q: str) -> bool:
    ql = q.lower()
    return ("employee" in ql) and (("most photos" in ql) or ("most pictures" in ql) or ("who has the most" in ql)))

def is_event_repeats_intent(q: str) -> bool:
    ql = q.lower()
    return ("event" in ql) and (("more than once" in ql) or ("repeated" in ql) or ("duplicates" in ql) or ("duplicate" in ql))

def is_bar_chart_by_state_intent(q: str) -> bool:
    ql = q.lower()
    return ("bar chart" in ql) and (("by state" in ql) or ("state" in ql))

def is_bar_chart_by_employee_last_intent(q: str) -> bool:
    ql = q.lower()
    return ("bar chart" in ql) and (("employee last name" in ql) or ("by employee" in ql))

# =============================
# HTTP Handler
# =============================
class handler(BaseHTTPRequestHandler):
    def _send(self, status: int, payload: dict):
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, X-API-Key")
        self.send_header("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
        self.end_headers()

    def do_GET(self):
        return self._send(200, {"status":"ok","config":config_status()})

    def do_POST(self):
        try:
            length = int(self.headers.get("Content-Length", 0))
            raw = self.rfile.read(length).decode("utf-8") if length else "{}"
            data = json.loads(raw or "{}")
        except Exception:
            return self._send(400, {"error":"Invalid JSON"})

        # Diagnostics
        if data.get("test"):
            payload = {"ok": True, **config_status()}
            if data.get("debug") == "airtable":
                sample = []
                try:
                    recs, _ = _airtable_list_records(page_size=1) if _airtable else ([], None)
                    for r in recs:
                        before = (r.get("fields", {}) or {}).copy()
                        after = (r.get("fields", {}) or {}).copy()
                        _normalize_photo_fields(after)
                        sample.append({"before": before, "after": after})
                except Exception as e:
                    sample = [{"error": str(e)}]
                payload["airtable_sample"] = sample
            return self._send(200, payload)

        question = (data.get("question") or "").strip()
        if not question:
            return self._send(400, {"error":"Missing 'question'"})

        ql = question.lower()
        use_airtable = ("photo" in ql) or ("airtable" in ql) or ("event" in ql)

        try:
            if use_airtable:
                state, overall_limit = parse_state_and_limit(question)

                # Intent: top employee by photos
                if is_employee_most_photos_intent(question):
                    try:
                        top, scanned = aggregate_top_employees(state=state, top_n=10)
                    except Exception as inner_e:
                        tb = traceback.format_exc()
                        return self._send(500, {"error": str(inner_e), "trace": tb, "context": {"state": state}})
                    if not top:
                        ans = "I didn’t find any photos."
                    else:
                        leader, leader_count = top[0]
                        ans = f"{leader} has the most photos with {leader_count}."
                        if len(top) > 1:
                            tail = "; ".join([f"{name} ({count})" for name, count in top[1:5]])
                            if tail:
                                ans += f" Next: {tail}."
                    return self._send(200, {
                        "answer": ans,
                        "query_type": "airtable",
                        "sql": None,
                        "raw_results": [],
                        "results_count": scanned,
                        "next_cursor": None
                    })

                # Intent: repeated events
                if is_event_repeats_intent(question):
                    try:
                        items, scanned = aggregate_repeated_events(state=state, min_count=2, top_n=25)
                    except Exception as inner_e:
                        tb = traceback.format_exc()
                        return self._send(500, {"error": str(inner_e), "trace": tb, "context": {"state": state}})
                    if not items:
                        ans = "No events were found more than once."
                    else:
                        ans = f"Found {len(items)} events that occurred more than once."
                    return self._send(200, {
                        "answer": ans,
                        "query_type": "airtable",
                        "sql": None,
                        "aggregations": {"type": "event_repeats", "items": items},
                        "raw_results": [],
                        "results_count": scanned,
                        "next_cursor": None
                    })

                # Intent: bar chart by state
                if is_bar_chart_by_state_intent(question):
                    labels, data_points, total = aggregate_counts_by_state(state=state)
                    ans = f"Photo counts by state (total {total})."
                    return self._send(200, {
                        "answer": ans,
                        "query_type": "airtable",
                        "sql": None,
                        "chart": {
                            "type": "bar",
                            "labels": labels,
                            "datasets": [{"label": "Photos", "data": data_points}]
                        },
                        "raw_results": [],
                        "results_count": total,
                        "next_cursor": None
                    })

                # Intent: bar chart by employee last name
                if is_bar_chart_by_employee_last_intent(question):
                    labels, data_points, total = aggregate_counts_by_employee_last_name(state=state)
                    ans = f"Photo counts by employee last name (total {total})."
                    return self._send(200, {
                        "answer": ans,
                        "query_type": "airtable",
                        "sql": None,
                        "chart": {
                            "type": "bar",
                            "labels": labels,
                            "datasets": [{"label": "Photos", "data": data_points}]
                        },
                        "raw_results": [],
                        "results_count": total,
                        "next_cursor": None
                    })

                # Default: paged photo listing
                cursor = data.get("cursor") or None
                page_size = data.get("page_size")
                try:
                    ps_default = max(1, min(100, AIRTABLE_PAGE_SIZE_DEFAULT))
                    ps = max(1, min(100, int(page_size))) if page_size else ps_default
                except Exception:
                    ps = max(1, min(100, AIRTABLE_PAGE_SIZE_DEFAULT))
                ps = min(ps, overall_limit)

                try:
                    rows, next_cursor = get_airtable_photos_page(state=state, page_size=ps, cursor=cursor)
                except Exception as inner_e:
                    tb = traceback.format_exc()
                    return self._send(500, {"error": str(inner_e), "trace": tb, "context": {"state": state, "page_size": ps, "cursor": cursor}})

                human_state = state or "any state"
                more = " (more available)" if next_cursor else ""
                answer = f"Returned {len(rows)} photos from {human_state}{more}."

                return self._send(200, {
                    "answer": answer,
                    "query_type": "airtable",
                    "sql": None,
                    "raw_results": rows,
                    "results_count": len(rows),
                    "next_cursor": next_cursor
                })

            # SQL path
            schema_hint = "(List allowed tables/views here)"
            candidate_sql = llm_generate_sql(question, schema_hint)
            if not is_safe_select(candidate_sql):
                return self._send(400, {"error":"Generated SQL failed safety checks","sql":candidate_sql})

            rows = run_sql(candidate_sql)
            answer = llm_format_answer(question, rows)
            return self._send(200, {
                "answer": answer,
                "query_type": "sql",
                "sql": candidate_sql,
                "raw_results": rows[:200],
                "results_count": len(rows),
                "next_cursor": None
            })

        except Exception as e:
            tb = traceback.format_exc()
            return self._send(500, {"error": str(e), "trace": tb})
