import os
import json
import re
from http.server import BaseHTTPRequestHandler

import openai
from pyairtable import Table
import pymssql

API_VERSION = "2025-08-29-photos-normalized-v3"

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

def _to_url_list(value):
    """
    Coerce Airtable attachment fields into a list of URL strings only.
    Accepts:
      - list of dicts with {'url': ...}
      - list of strings
      - single dict or single string
    Returns: list[str]
    """
    urls = []
    if not value:
        return urls
    items = value if isinstance(value, list) else [value]
    for x in items:
        if isinstance(x, str):
            # only consider http(s) strings
            if x.startswith("http"):
                urls.append(x)
        elif isinstance(x, dict):
            u = x.get("url")
            if isinstance(u, str) and u.startswith("http"):
                urls.append(u)
        # ignore everything else silently
    return urls

def _normalize_photo_fields(fields: dict):
    """
    Mutates `fields` so that any common photo/attachment field contains only URL strings.
    Also sets `first_photo_url` for convenience and ensures `Photo` exists as list[str].
    """
    candidates = ["Photo", "Photos", "Attachment", "Attachments", "Images", "Image"]
    found_urls = []

    for key in candidates:
        if key in fields:
            urls = _to_url_list(fields.get(key))
            fields[key] = urls
            if not found_urls and urls:
                found_urls = urls

    # Ensure a canonical Photo field exists
    if "Photo" not in fields:
        fields["Photo"] = found_urls

    # Convenience single URL
    fields["first_photo_url"] = found_urls[0] if found_urls else None

def parse_state_and_limit(question: str):
    ql = question.lower()
    # detect state
    state = None
    m = re.search(r"\b(ma|me|ri|vt)\b", ql)
    if m:
        state = m.group(1).upper()
    # detect limit
    limit = 10
    m2 = re.search(r"\b(past|last)\s+(\d+)", ql)
    if m2:
        try:
            limit = max(1, min(100, int(m2.group(2))))
        except Exception:
            pass
    return state, limit

def get_airtable_photos(state=None, limit=10):
    """Fetch Airtable rows, normalizing all photo/attachment fields to URL strings."""
    if not _airtable:
        return []

    # Build Airtable formula using DOUBLE quotes for string literals
    formula = None
    if state and state in STATE_CODES:
        # Try several field name variants; the first matching column will work
        name_variants = ["State", "state", "State Abbrev", "State Code"]
        clauses = [f'UPPER({{{f}}})="{state}"' for f in name_variants]
        formula = "OR(" + ",".join(clauses) + ")"

    # Sort newest by a likely date field (change if your field name differs)
    sorts = [{"field": "Date of Event", "direction": "desc"}]

    # NOTE: correct kwarg name is `formula=`, not filterByFormula
    records = _airtable.all(
        formula=formula,
        sort=sorts,
        max_records=limit
    )

    rows = []
    for rec in records:
        fields = rec.get("fields", {}) or {}
        _normalize_photo_fields(fields)  # <-- guarantees only string URLs in photo fields
        rows.append(fields)
    return rows

def fetch_airtable_all():
    """Generic fetch (also normalized) for non-photo questions."""
    if not _airtable:
        return []
    try:
        records = _airtable.all()
        rows = []
        for rec in records:
            fields = rec.get("fields", {}) or {}
            _normalize_photo_fields(fields)
            rows.append(fields)
        return rows
    except Exception:
        return []

# =============================
# Azure SQL Adapter
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
# Config Status (and version)
# =============================
def config_status():
    return {
        "api_version": API_VERSION,
        "airtable_configured": bool(_airtable),
        "sql_configured": bool(AZURE_SQL_SERVER and AZURE_SQL_DB and AZURE_SQL_USER and AZURE_SQL_PASSWORD),
        "openai_configured": bool(OPENAI_API_KEY),
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
# LLM Helpers
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
    # cleanup
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
        # Health check + version
        return self._send(200, {"status":"ok","config":config_status()})

    def do_POST(self):
        try:
            length = int(self.headers.get("Content-Length", 0))
            raw = self.rfile.read(length).decode("utf-8") if length else "{}"
            data = json.loads(raw or "{}")
        except Exception:
            return self._send(400, {"error":"Invalid JSON"})

        # System test / diagnostics
        if data.get("test"):
            payload = {"ok": True, **config_status()}
            if data.get("debug") == "airtable":
                sample = []
                try:
                    recs = _airtable.all(max_records=1) if _airtable else []
                    for r in recs:
                        f = r.get("fields", {}) or {}
                        # show what normalization would do
                        before = json.loads(json.dumps(f))  # shallow clone for readability
                        _normalize_photo_fields(f)
                        sample.append({"before": before, "after": f})
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
                rows = get_airtable_photos(state=state, limit=limit)
                answer = llm_format_answer(question, rows)
                return self._send(200, {
                    "answer": answer,
                    "query_type": "airtable",
                    "sql": None,
                    "raw_results": rows,   # already normalized to URL strings
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
            return self._send(500, {"error": str(e)})
