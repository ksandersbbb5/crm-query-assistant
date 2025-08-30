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

API_VERSION = "2025-08-29-airtable-paging-v12.1"

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
AIRTABLE_DEFAULT_LIMIT       = int(os.getenv("AIRTABLE_DEFAULT_LIMIT", "50"))
AIRTABLE_MAX_LIMIT           = int(os.getenv("AIRTABLE_MAX_LIMIT", "5000"))
AIRTABLE_SCAN_LIMIT          = int(os.getenv("AIRTABLE_SCAN_LIMIT", "2000"))
AIRTABLE_PAGE_SIZE_DEFAULT   = int(os.getenv("AIRTABLE_PAGE_SIZE_DEFAULT", "50"))

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

    limit = None
    m3 = re.search(r"\b(past|last|first|top)\s+(\d+)", question, flags=re.IGNORECASE)
    if m3:
        try:
            limit = int(m3.group(2))
        except Exception:
            pass

    if re.search(r"\ball\b", question, flags=re.IGNORECASE):
        limit = AIRTABLE_MAX_LIMIT

    if limit is None:
        limit = AIRTABLE_DEFAULT_LIMIT

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
    if not (AIRTABLE_API_KEY and AIRTABLE_BASE_ID and AIRTABLE_TABLE_NAME):
        return [], None
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{urlquote(AIRTABLE_TABLE_NAME)}"
    params = {}
    if formula:
        params["filterByFormula"] = formula
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
    formula = _build_formula_for_state(state)
    sort = ["-Date of Event"]
    try:
        recs, next_cursor = _airtable_list_records(formula=formula, sort=sort, page_size=page_size, offset=cursor)
    except HTTPError as http_err:
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
    collected = []
    cursor = None
    page_size = 100
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
        return f\"(Employee {sub[0]})\"
    return \"(Unknown)\"

def aggregate_top_employees(state=None, top_n=10):
    rows = fetch_airtable_records_for_aggregation(state=state, max_scan=AIRTABLE_SCAN_LIMIT)
    counter = Counter()
    for r in rows:
        photos = r.get(\"Photo\") or []
        if isinstance(photos, list) and photos:
            name = _extract_employee_name(r)
            counter[name] += 1
    top = counter.most_common(top_n)
    return top, len(rows)

def aggregate_counts_by_state(state=None, top_n=None):
    rows = fetch_airtable_records_for_aggregation(state=state, max_scan=AIRTABLE_SCAN_LIMIT)
    counter = Counter()
    for r in rows:
        photos = r.get(\"Photo\") or []
        if isinstance(photos, list) and photos:
            st = r.get(\"State\") or r.get(\"state\") or \"\"
            if isinstance(st, list) and st:
                st = st[0]
            st = (st or \"Unknown\").strip()
            counter[st] += 1
    items = counter.most_common(top_n) if top_n else counter.most_common()
    labels = [k for k, _ in items]
    data   = [v for _, v in items]
    return labels, data, sum(counter.values())

def aggregate_counts_by_employee_last_name(state=None, top_n=None):
    rows = fetch_airtable_records_for_aggregation(state=state, max_scan=AIRTABLE_SCAN_LIMIT)
    counter = Counter()
    for r in rows:
        photos = r.get(\"Photo\") or []
        if isinstance(photos, list) and photos:
            last = r.get(\"Employee Last Name\")
            if isinstance(last, list) and last:
                last = last[0]
            last = (last or \"Unknown\").strip()
            counter[last] += 1
    items = counter.most_common(top_n) if top_n else counter.most_common()
    labels = [k for k, _ in items]
    data   = [v for _, v in items]
    return labels, data, sum(counter.values())

def aggregate_repeated_events(state=None, min_count=2, top_n=25):
    rows = fetch_airtable_records_for_aggregation(state=state, max_scan=AIRTABLE_SCAN_LIMIT)
    groups = defaultdict(lambda: {\"count\": 0, \"states\": Counter(), \"dates\": []})
    for r in rows:
        name = (r.get(\"Event Name\") or \"\").strip()
        if not name:
            continue
        groups[name][\"count\"] += 1
        st = r.get(\"State\") or r.get(\"state\") or \"\"
        if isinstance(st, list) and st:
            st = st[0]
        st = (st or \"\").strip()
        if st:
            groups[name][\"states\"][st] += 1
        date = r.get(\"Date of Event\")
        if isinstance(date, str) and date:
            groups[name][\"dates\"].append(date)

    items = []
    for name, g in groups.items():
        if g[\"count\"] >= min_count:
            states_sorted = sorted(g[\"states\"].items(), key=lambda x: (-x[1], x[0]))
            items.append({\n                \"event_name\": name,\n                \"count\": g[\"count\"],\n                \"top_states\": [f\"{s} ({c})\" for s, c in states_sorted[:3]],\n                \"first_date\": min(g[\"dates\"]) if g[\"dates\"] else None,\n                \"last_date\": max(g[\"dates\"]) if g[\"dates\"] else None\n            })\n    items.sort(key=lambda x: (-x[\"count\"], x[\"event_name\"]))\n    return items[:top_n], len(rows)\n\n# =============================\n# SQL (unchanged)\n# =============================\n\ndef run_sql(sql: str):\n    conn = None\n    try:\n        conn = pymssql.connect(\n            server=AZURE_SQL_SERVER,\n            user=AZURE_SQL_USER,\n            password=AZURE_SQL_PASSWORD,\n            database=AZURE_SQL_DB,\n            login_timeout=5,\n            timeout=15,\n        )\n        cur = conn.cursor(as_dict=True)\n        cur.execute(sql)\n        rows = cur.fetchall()\n        return rows\n    finally:\n        try:\n            if conn:\n                conn.close()\n        except Exception:\n            pass\n\ndef config_status():\n    return {\n        \"api_version\": API_VERSION,\n        \"airtable_configured\": bool(_airtable),\n        \"sql_configured\": bool(AZURE_SQL_SERVER and AZURE_SQL_DB and AZURE_SQL_USER and AZURE_SQL_PASSWORD),\n        \"openai_configured\": bool(OPENAI_API_KEY),\n        \"disable_airtable_summary\": DISABLE_AIRTABLE_SUMMARY,\n        \"airtable_default_limit\": AIRTABLE_DEFAULT_LIMIT,\n        \"airtable_max_limit\": AIRTABLE_MAX_LIMIT,\n        \"airtable_scan_limit\": AIRTABLE_SCAN_LIMIT,\n        \"airtable_page_size_default\": AIRTABLE_PAGE_SIZE_DEFAULT,\n    }\n\n_SQL_BLOCKLIST = re.compile(\n    r\"(;|--|/\\*|\\*/|\\\\x| drop | alter | delete | insert | update | merge | exec | execute | xp_| sp_)\",\n    flags=re.IGNORECASE,\n)\n\ndef is_safe_select(sql: str) -> bool:\n    if not sql:\n        return False\n    if _SQL_BLOCKLIST.search(sql):\n        return False\n    if not re.match(r\"^\\s*select\\s\", sql, flags=re.IGNORECASE):\n        return False\n    return True\n\n# =============================\n# LLM (SQL only)\n# =============================\n\ndef llm_generate_sql(question: str, schema_hint: str = \"\") -> str:\n    if not OPENAI_API_KEY:\n        return \"SELECT TOP 10 * FROM INFORMATION_SCHEMA.TABLES\"\n    system = (\n        \"You translate natural-language CRM questions into a SINGLE, safe, read-only \"\n        \"T-SQL SELECT for Azure SQL Server. Use only tables/views mentioned in the schema hint. \"\n        \"Return ONLY the SQL, with no code fences, no comments, no CTEs, no variables, and no semicolons. \"\n        \"Always limit results with TOP 100.\"\n    )\n    user = f\"Question: {question}\\n\\nSchema hint:\\n{schema_hint}\"\n    resp = openai.ChatCompletion.create(\n        model=\"gpt-3.5-turbo\",\n        messages=[{\"role\":\"system\",\"content\":system},{\"role\":\"user\",\"content\":user}],\n        temperature=0.0,\n        max_tokens=300,\n    )\n    sql = resp.choices[0].message.content.strip()\n    sql = re.sub(r\"^```[a-zA-Z]*\", \"\", sql).strip()\n    sql = re.sub(r\"```$\", \"\", sql).strip()\n    sql = re.sub(r\"--.*?$\", \"\", sql, flags=re.MULTILINE)\n    sql = re.sub(r\"/\\*.*?\\*/\", \"\", sql, flags=re.DOTALL)\n    sql = sql.split(\";\")[0].strip()\n    if re.search(r\"\\bTOP\\s+\\d+\\b\", sql, flags=re.IGNORECASE) is None:\n        sql = re.sub(r\"^\\s*select\\s\", \"SELECT TOP 100 \", sql, flags=re.IGNORECASE)\n    return sql\n\ndef llm_format_answer(question: str, sample_rows: list) -> str:\n    if not sample_rows:\n        return \"No results found for your question.\"\n    if not OPENAI_API_KEY:\n        return json.dumps(sample_rows[:5], indent=2)\n    resp = openai.ChatCompletion.create(\n        model=\"gpt-3.5-turbo\",\n        messages=[\n            {\"role\":\"system\",\"content\":\"Summarize the data into a direct, business-friendly answer (1–2 sentences).\"},\n            {\"role\":\"user\",\"content\":f\"Question: {question}\\n\\nRows:\\n{json.dumps(sample_rows[:5], indent=2)}\"},\n        ],\n        temperature=0.2,\n        max_tokens=300,\n    )\n    return resp.choices[0].message.content.strip()\n\n# =============================\n# Intent detection (Airtable)\n# =============================\n\ndef is_employee_most_photos_intent(q: str) -> bool:\n    ql = q.lower()\n    return (\"employee\" in ql) and ((\"most photos\" in ql) or (\"most pictures\" in ql) or (\"who has the most\" in ql))\n\ndef is_event_repeats_intent(q: str) -> bool:\n    ql = q.lower()\n    return (\"event\" in ql) and ((\"more than once\" in ql) or (\"repeated\" in ql) or (\"duplicates\" in ql) or (\"duplicate\" in ql))\n\ndef is_bar_chart_by_state_intent(q: str) -> bool:\n    ql = q.lower()\n    return (\"bar chart\" in ql) and ((\"by state\" in ql) or (\"state\" in ql))\n\ndef is_bar_chart_by_employee_last_intent(q: str) -> bool:\n    ql = q.lower()\n    return (\"bar chart\" in ql) and ((\"employee last name\" in ql) or (\"by employee\" in ql))\n\n# =============================\n# HTTP Handler\n# =============================\nclass handler(BaseHTTPRequestHandler):\n    def _send(self, status: int, payload: dict):\n        body = json.dumps(payload).encode()\n        self.send_response(status)\n        self.send_header(\"Content-Type\", \"application/json\")\n        self.send_header(\"Access-Control-Allow-Origin\", \"*\")\n        self.send_header(\"Cache-Control\", \"no-store\")\n        self.end_headers()\n        self.wfile.write(body)\n\n    def do_OPTIONS(self):\n        self.send_response(204)\n        self.send_header(\"Access-Control-Allow-Origin\", \"*\")\n        self.send_header(\"Access-Control-Allow-Headers\", \"Content-Type, X-API-Key\")\n        self.send_header(\"Access-Control-Allow-Methods\", \"POST, GET, OPTIONS\")\n        self.end_headers()\n\n    def do_GET(self):\n        return self._send(200, {\"status\":\"ok\",\"config\":config_status()})\n\n    def do_POST(self):\n        try:\n            length = int(self.headers.get(\"Content-Length\", 0))\n            raw = self.rfile.read(length).decode(\"utf-8\") if length else \"{}\"\n            data = json.loads(raw or \"{}\")\n        except Exception:\n            return self._send(400, {\"error\":\"Invalid JSON\"})\n\n        # Diagnostics\n        if data.get(\"test\"):\n            payload = {\"ok\": True, **config_status()}\n            if data.get(\"debug\") == \"airtable\":\n                sample = []\n                try:\n                    recs, _ = _airtable_list_records(page_size=1) if _airtable else ([], None)\n                    for r in recs:\n                        before = (r.get(\"fields\", {}) or {}).copy()\n                        after = (r.get(\"fields\", {}) or {}).copy()\n                        _normalize_photo_fields(after)\n                        sample.append({\"before\": before, \"after\": after})\n                except Exception as e:\n                    sample = [{\"error\": str(e)}]\n                payload[\"airtable_sample\"] = sample\n            return self._send(200, payload)\n\n        question = (data.get(\"question\") or \"\").strip()\n        if not question:\n            return self._send(400, {\"error\":\"Missing 'question'\"})\n\n        ql = question.lower()\n        use_airtable = (\"photo\" in ql) or (\"airtable\" in ql) or (\"event\" in ql)\n\n        try:\n            if use_airtable:\n                state, overall_limit = parse_state_and_limit(question)\n\n                # Intent: top employee by photos\n                if is_employee_most_photos_intent(question):\n                    try:\n                        top, scanned = aggregate_top_employees(state=state, top_n=10)\n                    except Exception as inner_e:\n                        tb = traceback.format_exc()\n                        return self._send(500, {\"error\": str(inner_e), \"trace\": tb, \"context\": {\"state\": state}})\n                    if not top:\n                        ans = \"I didn’t find any photos.\"\n                    else:\n                        leader, leader_count = top[0]\n                        ans = f\"{leader} has the most photos with {leader_count}.\"\n                        if len(top) > 1:\n                            tail = \"; \".join([f\"{name} ({count})\" for name, count in top[1:5]])\n                            if tail:\n                                ans += f\" Next: {tail}.\"\n                    return self._send(200, {\n                        \"answer\": ans,\n                        \"query_type\": \"airtable\",\n                        \"sql\": None,\n                        \"raw_results\": [],\n                        \"results_count\": scanned,\n                        \"next_cursor\": None\n                    })\n\n                # Intent: repeated events\n                if is_event_repeats_intent(question):\n                    try:\n                        items, scanned = aggregate_repeated_events(state=state, min_count=2, top_n=25)\n                    except Exception as inner_e:\n                        tb = traceback.format_exc()\n                        return self._send(500, {\"error\": str(inner_e), \"trace\": tb, \"context\": {\"state\": state}})\n                    if not items:\n                        ans = \"No events were found more than once.\"\n                    else:\n                        ans = f\"Found {len(items)} events that occurred more than once.\"\n                    return self._send(200, {\n                        \"answer\": ans,\n                        \"query_type\": \"airtable\",\n                        \"sql\": None,\n                        \"aggregations\": {\"type\": \"event_repeats\", \"items\": items},\n                        \"raw_results\": [],\n                        \"results_count\": scanned,\n                        \"next_cursor\": None\n                    })\n\n                # Intent: bar chart by state\n                if is_bar_chart_by_state_intent(question):\n                    labels, data_points, total = aggregate_counts_by_state(state=state)\n                    ans = f\"Photo counts by state (total {total}).\"\n                    return self._send(200, {\n                        \"answer\": ans,\n                        \"query_type\": \"airtable\",\n                        \"sql\": None,\n                        \"chart\": {\n                            \"type\": \"bar\",\n                            \"labels\": labels,\n                            \"datasets\": [{\"label\": \"Photos\", \"data\": data_points}]\n                        },\n                        \"raw_results\": [],\n                        \"results_count\": total,\n                        \"next_cursor\": None\n                    })\n\n                # Intent: bar chart by employee last name\n                if is_bar_chart_by_employee_last_intent(question):\n                    labels, data_points, total = aggregate_counts_by_employee_last_name(state=state)\n                    ans = f\"Photo counts by employee last name (total {total}).\"\n                    return self._send(200, {\n                        \"answer\": ans,\n                        \"query_type\": \"airtable\",\n                        \"sql\": None,\n                        \"chart\": {\n                            \"type\": \"bar\",\n                            \"labels\": labels,\n                            \"datasets\": [{\"label\": \"Photos\", \"data\": data_points}]\n                        },\n                        \"raw_results\": [],\n                        \"results_count\": total,\n                        \"next_cursor\": None\n                    })\n\n                # Default: paged photo listing\n                cursor = data.get(\"cursor\") or None\n                page_size = data.get(\"page_size\")\n                try:\n                    ps_default = max(1, min(100, AIRTABLE_PAGE_SIZE_DEFAULT))\n                    ps = max(1, min(100, int(page_size))) if page_size else ps_default\n                except Exception:\n                    ps = max(1, min(100, AIRTABLE_PAGE_SIZE_DEFAULT))\n                ps = min(ps, overall_limit)\n\n                try:\n                    rows, next_cursor = get_airtable_photos_page(state=state, page_size=ps, cursor=cursor)\n                except Exception as inner_e:\n                    tb = traceback.format_exc()\n                    return self._send(500, {\"error\": str(inner_e), \"trace\": tb, \"context\": {\"state\": state, \"page_size\": ps, \"cursor\": cursor}})\n\n                human_state = state or \"any state\"\n                more = \" (more available)\" if next_cursor else \"\"\n                answer = f\"Returned {len(rows)} photos from {human_state}{more}.\"\n\n                return self._send(200, {\n                    \"answer\": answer,\n                    \"query_type\": \"airtable\",\n                    \"sql\": None,\n                    \"raw_results\": rows,\n                    \"results_count\": len(rows),\n                    \"next_cursor\": next_cursor\n                })\n\n            # SQL path\n            schema_hint = \"(List allowed tables/views here)\"\n            candidate_sql = llm_generate_sql(question, schema_hint)\n            if not is_safe_select(candidate_sql):\n                return self._send(400, {\"error\":\"Generated SQL failed safety checks\",\"sql\":candidate_sql})\n\n            rows = run_sql(candidate_sql)\n            answer = llm_format_answer(question, rows)\n            return self._send(200, {\n                \"answer\": answer,\n                \"query_type\": \"sql\",\n                \"sql\": candidate_sql,\n                \"raw_results\": rows[:200],\n                \"results_count\": len(rows),\n                \"next_cursor\": None\n            })\n\n        except Exception as e:\n            tb = traceback.format_exc()\n            return self._send(500, {\"error\": str(e), \"trace\": tb})\n```

---

## What you should see
1. Replace `api/query.py` with the file above, redeploy.
2. Hard-refresh the app and hit **Run System Test** — it should show:
