import os
import json
import re
from http.server import BaseHTTPRequestHandler

import openai
from pyairtable import Table
import pymssql

# -----------------------------
# Env Vars (AZURE_* and AIRTABLE_*)
# -----------------------------
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

AIRTABLE_API_KEY    = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID    = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME")

AZURE_SQL_SERVER   = os.getenv("AZURE_SQL_SERVER")
AZURE_SQL_DB       = os.getenv("AZURE_SQL_DB")
AZURE_SQL_USER     = os.getenv("AZURE_SQL_USER")
AZURE_SQL_PASSWORD = os.getenv("AZURE_SQL_PASSWORD")

openai.api_key = OPENAI_API_KEY

# -----------------------------
# Airtable adapter
# -----------------------------
_airtable = None
if AIRTABLE_API_KEY and AIRTABLE_BASE_ID and AIRTABLE_TABLE_NAME:
    try:
        _airtable = Table(AIRTABLE_API_KEY, AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)
    except Exception:
        _airtable = None

STATE_CODES = {"MA", "ME", "RI", "VT"}

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
    """Fetch Airtable photo rows, normalized to Photo URLs."""
    if not _airtable:
        return []

    formula = None
    if state:
        # Try multiple field name variants for safety
        candidates = ["State", "state", "State Abbrev", "State Code"]
        clauses = [f'UPPER({{{f}}})="{state}"' for f in candidates]
        formula = "OR(" + ",".join(clauses) + ")"

    sorts = [{"field": "Date of Event", "direction": "desc"}]

    records = _airtable.all(
        formula=formula,
        sort=sorts,
        max_records=limit
    )

    rows = []
    for rec in records:
        fields = rec.get("fields", {}) or {}
        urls = []
        for a in fields.get("Photo", []) or []:
            if isinstance(a, dict) and a.get("url"):
                urls.append(a["url"])
            elif isinstance(a, str):
                urls.append(a)
        fields["Photo"] = urls
        rows.append(fields)

    return rows

# -----------------------------
# Azure SQL adapter
# -----------------------------
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

# -----------------------------
# Config status
# -----------------------------
def config_status():
    return {
        "airtable_configured": bool(_airtable),
        "sql_configured": bool(AZURE_SQL_SERVER and AZURE_SQL_DB and AZURE_SQL_USER and AZURE_SQL_PASSWORD),
        "openai_configured": bool(OPENAI_API_KEY),
    }

# -----------------------------
# SQL safety
# -----------------------------
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

# -----------------------------
# LLM helpers
# -----------------------------
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

# -----------------------------
# HTTP handler
# -----------------------------
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

        if data.get("test"):
            return self._send(200, {"ok": True, **config_status()})

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
                    "raw_results": rows,
                    "results_count": len(rows),
                })

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
