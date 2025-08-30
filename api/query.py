import os
import json
import re
import traceback
from collections import Counter
from http.server import BaseHTTPRequestHandler

import openai
from pyairtable import Table
import pymssql
from requests.exceptions import HTTPError

API_VERSION = "2025-08-29-photos-normalized-v9"

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

DISABLE_AIRTABLE_SUMMARY = (os.getenv("DISABLE_AIRTABLE_SUMMARY", "true").lower() == "true")
AIRTABLE_SCAN_LIMIT = int(os.getenv("AIRTABLE_SCAN_LIMIT", "2000"))  # cap total rows to scan for aggregations

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
    """Coerce attachments to list[str] without using .startswith on unknown types."""
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
    """Ensure photo fields are URL strings only; add first_photo_url."""
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
    """Find state and limit without confusing 'show me' with state ME."""
    m = re.search(
        r"\b(?:from|in)\s+(massachusetts|maine|rhode island|vermont|ma|me|ri|vt)\b",
        question,
        flags=re.IGNORECASE,
    )
    state = None
    if m:
        state = STATE_NAME_TO_CODE[m.group(1).lower()]

    if not state:
        m2 = re.search(r"\b(MA|ME|RI|VT)\b", question)
        if m2:
            state = m2.group(1).upper()

    limit = 10
    m3 = re.search(r"\b(past|last)\s+(\d+)", question, flags=re.IGNORECASE)
    if m3:
        try:
            limit = max(1, min(100, int(m3.group(2))))
        except Exception:
            pass
    return state, limit

def _discover_columns():
    """Fetch 1 record (no formula) to see what columns exist."""
    try:
        recs = _airtable.all(max_records=1) if _airtable else []
        if recs:
            fields = recs[0].get("fields", {}) or {}
            return set(fields.keys())
    except Exception:
        pass
    return set()

def get_airtable_photos(state=None, limit=10):
    """Fetch Airtable rows, normalized to URL strings. Auto-detects real field names for filters."""
    if not _airtable:
        return []

    formula = None
    if state and state in STATE_CODES:
        existing = _discover_columns()
        candidates = ["State", "state", "State Abbrev", "State Code"]
        usable = [f for f in candidates if f in existing]
        if usable:
            clauses = [f'UPPER({{{f}}})="{state}"' for f in usable]
            formula = "OR(" + ",".join(clauses) + ")"

    sort = ["-Date of Event"]  # adjust if your base uses a different field

    try:
        records = _airtable.all(formula=formula, sort=sort, max_records=limit)
    except HTTPError as http_err:
        if hasattr(http_err.response, "status_code") and http_err.response.status_code == 422:
            records = _airtable.all(sort=sort, max_records=limit)
        else:
            raise

    rows = []
    for rec in records:
        fields = rec.get("fields", {}) or {}
        _normalize_photo_fields(fields)
        rows.append(fields)
    return rows

def fetch_airtable_records_for_aggregation(state=None, max_scan=AIRTABLE_SCAN_LIMIT):
    """
    Pull a larger window of records for aggregation (e.g., top employees).
    Applies state filter if we can build a valid formula; otherwise scans unfiltered.
    """
    if not _airtable:
        return []

    formula = None
    if state and state in STATE_CODES:
        existing = _discover_columns()
        candidates = ["State", "state", "State Abbrev", "State Code"]
        usable = [f for f in candidates if f in existing]
        if usable:
            clauses = [f'UPPER({{{f}}})="{state}"' for f in usable]
            formula = "OR(" + ",".join(clauses) + ")"

    sort = ["-Date of Event"]

    # Pull up to max_scan rows (Airtable all() will respect max_records)
    try:
        records = _airtable.all(formula=formula, sort=sort, max_records=max_scan)
    except HTTPError as http_err:
        if hasattr(http_err.response, "status_code") and http_err.response.status_code == 422:
            records = _airtable.all(sort=sort, max_records=max_scan)
        else:
            raise

    rows = []
    for rec in records:
        fields = rec.get("fields", {}) or {}
        _normalize_photo_fields(fields)
        rows.append(fields)
    return rows

def _extract_employee_name(fields: dict):
    """
    Your base has Employee First/Last Name as arrays (lookup) in some rows.
    Accept either string or [string]. Fallback to 'Submitted by Employee' id.
    """
    first = fields.get("Employee First Name")
    last = fields.get("Employee Last Name")

    def _first_string(x):
        if isinstance(x, list) and x:
            return _safe_to_str(x[0]).strip()
        return _safe_to_str(x).strip()

    f = _first_string(first)
    l = _first_string(last)
    if f or l:
        return (l + ", " + f).strip(", ").strip()

    # Fallback: use submitter record id
    sub = fields.get("Submitted by Employee")
    if isinstance(sub, list) and sub:
        return f"(Employee {sub[0]})"
    return "(Unknown)"

def aggregate_top_employees(state=None, top_n=10):
    rows = fetch_airtable_records_for_aggregation(state=state, max_scan=AIRTABLE_SCAN_LIMIT)
    counter = Counter()
    for r in rows:
        name = _extract_employee_name(r)
        # Count one per record that actually has at least one photo URL
        photos = r.get("Photo") or []
        if isinstance(photos, list) and photos:
            counter[name] += 1
    top = counter.most_common(top_n)
    return top, len(rows)

# =============================
# Azure SQL Adapter (unchanged)
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

# =============================
# Config Status
# =============================
def config_status():
    return {
        "api_version": API_VERSION,
        "airtable_configured": bool(_airtable),
        "sql_configured": bool(AZURE_SQL_SERVER and AZURE_SQL_DB and AZURE_SQL_USER and AZURE_SQL_PASSWORD),
        "openai_configured": bool(OPENAI_API_KEY),
        "disable_airtable_summary": DISABLE_AIRTABLE_SUMMARY,
        "airtable_scan_limit": AIRTABLE_SCAN_LIMIT,
    }

# =============================
# SQL Safety (read-only)
# =============================
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
# LLM Helpers (SQL only)
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
    return ("employee" in ql) and (("most photos" in ql) or ("most pictures" in ql) or ("who has the most" in ql))

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
                    recs = _airtable.all(max_records=1) if _airtable else []
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
        use_airtable = ("photo" in ql) or ("airtable" in ql)

        try:
            if use_airtable:
                state, limit = parse_state_and_limit(question)

                # INTENT: Which employee has the most photos?
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
                        "raw_results": [],  # not needed for this intent
                        "results_count": scanned
                    })

                # Default Airtable path (e.g., photos grid queries)
                try:
                    rows = get_airtable_photos(state=state, limit=limit)
                except Exception as inner_e:
                    tb = traceback.format_exc()
                    return self._send(500, {"error": str(inner_e), "trace": tb, "context": {"state": state, "limit": limit}})

                if DISABLE_AIRTABLE_SUMMARY:
                    human_state = state or "any state"
                    answer = f"Found {len(rows)} photos from {human_state}."
                else:
                    answer = llm_format_answer(question, rows)

                return self._send(200, {
                    "answer": answer,
                    "query_type": "airtable",
                    "sql": None,
                    "raw_results": rows[:10],
                    "results_count": len(rows),
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
            })

        except Exception as e:
            tb = traceback.format_exc()
            return self._send(500, {"error": str(e), "trace": tb})
