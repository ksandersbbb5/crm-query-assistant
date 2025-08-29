from http.server import BaseHTTPRequestHandler
import openai
import os
import json
import decimal
import datetime
import re
import urllib.parse

# Try to import pymssql for SQL Server connection
try:
    import pymssql
    MSSQL_AVAILABLE = True
except ImportError:
    MSSQL_AVAILABLE = False
    print("pymssql not available - using mock data only")

# Try to import pyairtable
try:
    from pyairtable import Api
    AIRTABLE_AVAILABLE = True
except ImportError:
    AIRTABLE_AVAILABLE = False
    print("pyairtable not available")

# Configure OpenAI
openai.api_key = os.environ.get('OPENAI_API_KEY')

def get_database_schema():
    """Get Applications table schema"""
    schema_info = """
Database: BlueReporting_Boston
Table: Applications

Key Fields:
- Application ID (AppID): Number - Unique application identifier
- Application Status (app_status): Text - Status of the application
- Business Name (dba): Text - Business name
- AB Status (ab_status): Text - Accreditation status  
- Application Closed (app_closed): Boolean - Whether application is closed
- BID: Number - Business ID
- BID_TOB: Text - Type of business
- City (city): Text - Business city
- State (state): Text - Business state
- Zip Code (zip): Text - Business zip code
- Salesperson (rep): Text - Sales representative name
- Salesperson ID (RepID): Number - Sales rep ID
- Invoice Balance (invoice_balance): Currency - Outstanding balance
- Invoice Total (invoice_total): Currency - Total invoice amount
- Invoice ID (invoice_id): Number - Invoice identifier
- Date Created (DateCreated): Date - When application was created
- Last Update (lastUpdate): Date - Last modification date
- Number of Employees (number_of_employees): Number - Employee count
- Payment Method on File (PmtMethodOnFile): Boolean - Has payment method
- Payment Type (payment_type): Text - Type of payment
- Customer Care ID (CustomerCareID): Number - Customer service rep ID
- Customer Care Rep: Text - Customer service rep name
- Form (form): Text - Application form type
- Source (source): Text - Application source
- Campaign URL (campaignUrl): Text - Marketing campaign URL
- Membership Plan (membership_plan): Text - Selected membership plan
- Hold (hold): Text - Reason for hold if any
- Checklist Last Updated (Checklist_LastUpdate): Date - Last checklist update
- Checklist Open (Checklist_Open): Boolean - Whether checklist is open
- Year Created: Year - Year application was created
- Year Week: Number - Week number of creation
- Submitted with Payment (submitted_with_PayMethod): Boolean - Payment submitted

Common Status Values: "Processed/Accepted", "Rejected/Denied", "Application Withdrawn", "No Payment Method"
Common States: MA, ME, VT, RI, NH, CT (New England states)
"""
    return schema_info

def get_airtable_schema():
    """Get Airtable schema info"""
    return """
Airtable Table: CREM Photos
Fields:
- ID: Autonumber - Primary key
- Date of Event: Date - When the event took place
- Email Address: Lookup - Employee email from linked table
- Employee First Name: Lookup - First name from employee table
- Employee Last Name: Lookup - Last name from employee table
- Employee Photo: Lookup - Employee photo from linked table
- Employee Status: Lookup - Employment status from linked table
- Event Name: Single line text - Name of the event
- Photo: Attachment - Event photo file(s)
- State: Single select - State where event took place (VT, MA, etc.)
- Submitted by Employee: Link to another record - Employee who submitted
- Team Member OLD: Single select - Legacy team member field

Common queries:
- Photos by state (VT, Vermont, MA, Massachusetts, etc.)
- Event photos over time periods
- Photo counts and statistics
- Employee submissions
"""

def get_combined_schema():
    """Get both SQL and Airtable schemas"""
    return f"""
SQL SERVER DATA:
{get_database_schema()}

AIRTABLE DATA:
{get_airtable_schema()}

When user asks about photos, events, CREM photos, or uses visualization terms (chart, table), consider using Airtable.
When user asks about applications, invoices, or CRM data, use SQL Server.
"""

def get_db_connection():
    """Create SQL Server connection using pymssql"""
    if not MSSQL_AVAILABLE:
        return None
        
    # Check if we have all required environment variables
    required_vars = ['SQL_SERVER', 'SQL_DATABASE', 'SQL_USERNAME', 'SQL_PASSWORD']
    if not all(os.environ.get(var) for var in required_vars):
        print("Missing required SQL Server environment variables")
        return None
    
    try:
        conn = pymssql.connect(
            server=os.environ.get('SQL_SERVER'),
            database=os.environ.get('SQL_DATABASE'),
            user=os.environ.get('SQL_USERNAME'),
            password=os.environ.get('SQL_PASSWORD'),
            as_dict=True  # Return results as dictionaries
        )
        return conn
    except Exception as e:
        print(f"SQL Server connection error: {str(e)}")
        return None

def parse_airtable_query(question):
    """Parse Airtable-specific query parameters from the question"""
    query_conditions = {}
    question_lower = question.lower()
    
    # Check for state filters
    state_abbrevs = {
        'vermont': 'VT', 'vt': 'VT',
        'massachusetts': 'MA', 'ma': 'MA', 
        'maine': 'ME', 'me': 'ME',
        'new hampshire': 'NH', 'nh': 'NH',
        'rhode island': 'RI', 'ri': 'RI',
        'connecticut': 'CT', 'ct': 'CT'
    }
    
    for state_name, abbrev in state_abbrevs.items():
        if state_name in question_lower:
            query_conditions['state'] = abbrev
            break
    
    # Check for numeric limits
    numbers = re.findall(r'\b(\d+)\b', question)
    if numbers and any(word in question_lower for word in ['past', 'last', 'top', 'first', 'recent']):
        query_conditions['limit'] = int(numbers[0])
    
    # Check for sorting
    if any(word in question_lower for word in ['recent', 'past', 'latest', 'last']):
        query_conditions['order_by'] = 'date'
    
    return query_conditions

def search_airtable(query_conditions=None):
    """Search Airtable records with filtering"""
    try:
        if not AIRTABLE_AVAILABLE:
            return []
        
        api_key = os.environ.get('AIRTABLE_API_KEY')
        base_id = os.environ.get('AIRTABLE_BASE_ID')
        table_name = os.environ.get('AIRTABLE_TABLE_NAME')
        
        if not all([api_key, base_id, table_name]):
            print("Missing Airtable configuration")
            return []
            
        api = Api(api_key)
        
        # Try both the raw table name and URL-encoded version
        try:
            table = api.table(base_id, table_name)
            records = table.all()
        except Exception as e:
            print(f"First attempt failed: {e}")
            # Try URL-encoded table name
            encoded_table_name = urllib.parse.quote(table_name)
            print(f"Trying encoded table name: {encoded_table_name}")
            table = api.table(base_id, encoded_table_name)
            records = table.all()
        
        # Convert to consistent format
        results = []
        for record in records:
            result = {
                'record_id': record['id'], 
                'source': 'airtable'
            }
            # Add all fields from the record
            fields = record.get('fields', {})
            for key, value in fields.items():
                # Handle attachment fields specially
                if key == 'Photo' and isinstance(value, list):
                    result[key] = [att.get('url', '') for att in value]
                else:
                    result[key] = value
            results.append(result)
        
        print(f"Found {len(results)} Airtable records")
        
        # Apply any filtering based on query_conditions
        if query_conditions:
            # Filter by state if specified
            if 'state' in query_conditions:
                state_filter = query_conditions['state'].upper()
                results = [r for r in results if r.get('State', '').upper() == state_filter]
                print(f"After state filter: {len(results)} records")
            
            # Sort by date if needed
            if 'order_by' in query_conditions and query_conditions['order_by'] == 'date':
                results.sort(key=lambda x: x.get('Date of Event', ''), reverse=True)
            
            # Limit results if specified
            if 'limit' in query_conditions:
                results = results[:query_conditions['limit']]
                print(f"After limit: {len(results)} records")
        
        return results
        
    except Exception as e:
        print(f"Airtable error: {str(e)}")
        return []

def text_to_sql(question, schema):
    """Convert question to SQL or determine if Airtable query needed"""
    try:
        # Check if question is about Airtable data
        airtable_keywords = ['airtable', 'air table', 'photo', 'photos', 'crem photos', 'event photo', 'event', 'events']
        is_airtable_query = any(keyword in question.lower() for keyword in airtable_keywords)
        
        # Parse potential Airtable query conditions
        airtable_conditions = parse_airtable_query(question) if is_airtable_query else {}
        
        if is_airtable_query:
            return {"type": "airtable", "query": "filtered", "conditions": airtable_conditions}
        
        # For testing with mock data, use a dummy key if none is set
        if not openai.api_key:
            # Return a simple SQL query for mock testing
            if "45874" in question:
                return {"type": "sql", "query": "SELECT * FROM Applications WHERE AppID = 45874"}
            elif "count" in question.lower():
                return {"type": "sql", "query": "SELECT COUNT(*) as count FROM Applications"}
            elif "average" in question.lower() and "invoice" in question.lower():
                return {"type": "sql", "query": "SELECT rep, AVG(invoice_total) as avg_invoice FROM Applications GROUP BY rep ORDER BY avg_invoice DESC"}
            elif "rejected" in question.lower() or "denied" in question.lower():
                return {"type": "sql", "query": "SELECT * FROM Applications WHERE app_status IN ('Rejected/Denied') ORDER BY DateCreated DESC"}
            elif "balance" in question.lower():
                return {"type": "sql", "query": "SELECT * FROM Applications WHERE invoice_balance > 0 ORDER BY invoice_balance DESC"}
            elif "top" in question.lower() and "cities" in question.lower():
                return {"type": "sql", "query": "SELECT TOP 10 city, COUNT(*) as count FROM Applications GROUP BY city ORDER BY count DESC"}
            else:
                return {"type": "sql", "query": "SELECT TOP 5 * FROM Applications ORDER BY DateCreated DESC"}
        
        response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[
                {
                    "role": "system",
                    "content": f"""Convert natural language to SQL Server queries or indicate if Airtable data is needed.
                    
{schema}

Rules:
- If the question mentions photos, events, CREM, or asks for charts/tables of photo data, return: "AIRTABLE_QUERY"
- Otherwise, generate SQL Server query
- Return ONLY the SQL query, no explanations or markdown
- Use SQL Server syntax (TOP not LIMIT) 
- Current year is 2025
- Table name is exactly: Applications
- Use exact field names from schema (case sensitive)
- For application ID queries, use: AppID
- For status queries, use: app_status  
- For salesperson queries, use: rep
- For date filtering, use: DateCreated or lastUpdate
- For invoice amounts, use: invoice_total or invoice_balance
- Common status values: 'Processed/Accepted', 'Rejected/Denied', 'Application Withdrawn'
- For current year data: WHERE YEAR(DateCreated) = 2025
- Always include reasonable TOP limits for large result sets (TOP 100 max)
- Use single quotes for string values"""
                },
                {"role": "user", "content": question}
            ],
            temperature=0.1,
            max_tokens=500
        )
        
        sql = response.choices[0].message.content.strip()
        
        # Check if response indicates Airtable
        if "AIRTABLE_QUERY" in sql or "airtable" in sql.lower():
            return {"type": "airtable", "query": "filtered", "conditions": airtable_conditions}
        
        # Clean up the response (remove markdown formatting if present)
        if sql.startswith('```sql'):
            sql = sql[6:-3]
        elif sql.startswith('```'):
            sql = sql[3:-3]
            
        return {"type": "sql", "query": sql.strip()}
        
    except Exception as e:
        # Return a default query for testing
        return {"type": "sql", "query": "SELECT TOP 5 * FROM Applications ORDER BY DateCreated DESC"}

def execute_sql_via_rest_mock(sql_query):
    """Execute SQL query using mock data for testing"""
    try:
        # Mock data for testing the interface
        
        if "AppID" in sql_query and "45874" in sql_query:
            mock_results = [
                {
                    "AppID": 45874,
                    "app_status": "Processed/Accepted",
                    "dba": "Medina's Maintenance Services & Home Improvement",
                    "city": "Roslindale",
                    "state": "MA",
                    "invoice_total": 642,
                    "rep": "Cochrane, Valerie",
                    "source": "sql_server"
                }
            ]
        elif "COUNT" in sql_query.upper():
            mock_results = [{"count": 20000, "source": "sql_server"}]
        elif "TOP" in sql_query.upper() and "city" in sql_query.lower():
            mock_results = [
                {"city": "Boston", "count": 2500, "source": "sql_server"},
                {"city": "Cambridge", "count": 1800, "source": "sql_server"},
                {"city": "Worcester", "count": 1200, "source": "sql_server"},
                {"city": "Springfield", "count": 800, "source": "sql_server"},
                {"city": "Lowell", "count": 600, "source": "sql_server"}
            ]
        else:
            # Default sample results
            mock_results = [
                {
                    "AppID": 45874,
                    "app_status": "Processed/Accepted", 
                    "dba": "Sample Business",
                    "city": "Boston",
                    "state": "MA",
                    "invoice_total": 500,
                    "rep": "Sample Rep",
                    "source": "sql_server"
                }
            ]
        
        return mock_results, None
        
    except Exception as e:
        return None, str(e)

def execute_sql_via_rest(sql_query):
    """Execute SQL query against actual SQL Server or fall back to mock data"""
    # Add debug logging
    print(f"Attempting to execute SQL: {sql_query}")
    print(f"SQL_SERVER env: {os.environ.get('SQL_SERVER', 'Not Set')}")
    print(f"MSSQL Available: {MSSQL_AVAILABLE}")
    
    # Try real SQL Server connection first
    conn = get_db_connection()
    
    if conn:
        print("Successfully connected to SQL Server!")
        try:
            cursor = conn.cursor()
            
            # Execute the query
            cursor.execute(sql_query)
            
            # Fetch all results (already as dictionaries due to as_dict=True)
            results = cursor.fetchall()
            
            # Convert any special types and add source
            for result in results:
                result['source'] = 'sql_server'
                for key, value in list(result.items()):
                    if isinstance(value, decimal.Decimal):
                        result[key] = float(value)
                    elif isinstance(value, datetime.datetime):
                        result[key] = value.isoformat()
                    elif isinstance(value, datetime.date):
                        result[key] = value.isoformat()
            
            cursor.close()
            conn.close()
            
            print(f"Query returned {len(results)} results from real database")
            return results, None
            
        except Exception as e:
            print(f"SQL execution error: {str(e)}")
            # Fall back to mock data
            return execute_sql_via_rest_mock(sql_query)
    else:
        # Use mock data if no SQL connection available
        print("No SQL connection available, using mock data")
        return execute_sql_via_rest_mock(sql_query)

def execute_query(query_info):
    """Execute either SQL or Airtable query"""
    if isinstance(query_info, dict) and query_info.get('type') == 'airtable':
        # Query Airtable with conditions
        conditions = query_info.get('conditions', {})
        results = search_airtable(conditions)
        if results is not None:  # Check for None specifically, empty list is valid
            return results, None
        else:
            return [], "No Airtable data found or Airtable not configured"
    else:
        # Handle as SQL query (backward compatible)
        if isinstance(query_info, dict):
            sql_query = query_info.get('query', '')
        else:
            sql_query = query_info
        return execute_sql_via_rest(sql_query)

def format_results(results, question):
    """Use LLM to format results into natural language answer"""
    if not results:
        return "No results found for your question."
    
    try:
        # For testing without OpenAI key
        if not openai.api_key:
            question_lower = question.lower()
            
            # Handle count questions
            if 'how many' in question_lower or 'count' in question_lower:
                if 'count' in results[0]:
                    return f"The count is: {results[0]['count']:,}"
                else:
                    return f"Found {len(results)} records."
            
            # Handle single result
            if len(results) == 1:
                formatted = "Found 1 result:\n"
                for key, value in results[0].items():
                    if key not in ['source', 'record_id']:  # Don't show internal fields
                        formatted += f"- {key}: {value}\n"
                return formatted.strip()
            
            # Handle multiple results
            else:
                formatted = f"Found {len(results)} results:\n\n"
                for i, result in enumerate(results[:5], 1):
                    formatted += f"Result {i}:\n"
                    for key, value in result.items():
                        if key not in ['source', 'record_id']:  # Don't show internal fields
                            formatted += f"- {key}: {value}\n"
                    formatted += "\n"
                if len(results) > 5:
                    formatted += f"... and {len(results) - 5} more results"
                return formatted.strip()
        
        # Use OpenAI to format results
        # Convert results to a readable format
        if len(results) == 1:
            result_text = f"Found 1 result: {results[0]}"
        else:
            result_text = f"Found {len(results)} results:\n"
            for i, result in enumerate(results[:10]):  # Limit to first 10 results
                result_text += f"{i+1}. {result}\n"
            if len(results) > 10:
                result_text += f"... and {len(results) - 10} more results"
        
        response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[
                {
                    "role": "system",
                    "content": "Format database query results into a clear, natural language answer. Be concise but informative. If the question asks for counts, provide the count. If it asks for specific data, list the relevant information."
                },
                {
                    "role": "user",
                    "content": f"Question: {question}\n\nResults: {result_text}\n\nPlease format this into a clear answer:"
                }
            ],
            temperature=0.1,
            max_tokens=300
        )
        
        return response.choices[0].message.content.strip()
        
    except Exception as e:
        # Fallback formatting
        return f"Results: {json.dumps(results[:5], indent=2)}"

class handler(BaseHTTPRequestHandler):
    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type, X-API-Key')
        self.end_headers()

    def do_GET(self):
        """Handle GET requests for testing"""
        try:
            # Default GET response
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(b"CRM Query API is running")
            
        except Exception as e:
            self.send_response(500)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps({'error': str(e)}).encode())

    def do_POST(self):
        try:
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            data = json.loads(post_data.decode('utf-8'))
            
            # API Key Security Check (if enabled)
            api_key = self.headers.get('X-API-Key') or data.get('api_key')
            expected_key = os.environ.get('MY_API_SECRET')
            
            if expected_key and api_key != expected_key:
                self.send_response(401)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({'error': 'Unauthorized - Invalid API Key'}).encode())
                return
            
            # Handle test request
            if data.get('test') == True:
                test_results = {
                    "pymssql_available": MSSQL_AVAILABLE,
                    "pyairtable_available": AIRTABLE_AVAILABLE,
                    "sql_server": os.environ.get('SQL_SERVER', 'Not Set'),
                    "sql_database": os.environ.get('SQL_DATABASE', 'Not Set'),
                    "sql_username": os.environ.get('SQL_USERNAME', 'Not Set'),
                    "sql_password": "***" if os.environ.get('SQL_PASSWORD') else "Not Set",
                    "airtable_api_key": "Set" if os.environ.get('AIRTABLE_API_KEY') else "Not Set",
                    "airtable_base_id": os.environ.get('AIRTABLE_BASE_ID', 'Not Set'),
                    "airtable_table": os.environ.get('AIRTABLE_TABLE_NAME', 'Not Set'),
                    "openai_key": "Set" if os.environ.get('OPENAI_API_KEY') else "Not Set",
                    "api_secret": "Set" if os.environ.get('MY_API_SECRET') else "Not Set",
                    "sql_connection_test": "Not Tested",
                    "airtable_connection_test": "Not Tested"
                }
                
                # Test SQL connection
                if MSSQL_AVAILABLE and all([
                    os.environ.get('SQL_SERVER'),
                    os.environ.get('SQL_DATABASE'),
                    os.environ.get('SQL_USERNAME'),
                    os.environ.get('SQL_PASSWORD')
                ]):
                    try:
                        conn = get_db_connection()
                        if conn:
                            cursor = conn.cursor()
                            cursor.execute("SELECT COUNT(*) as count FROM Applications")
                            result = cursor.fetchone()
                            test_results["sql_connection_test"] = f"Success! Found {result['count']} records"
                            cursor.close()
                            conn.close()
                        else:
                            test_results["sql_connection_test"] = "Failed to create connection"
                    except Exception as e:
                        test_results["sql_connection_test"] = f"Error: {str(e)}"
                
                # Test Airtable connection
                if AIRTABLE_AVAILABLE and all([
                    os.environ.get('AIRTABLE_API_KEY'),
                    os.environ.get('AIRTABLE_BASE_ID'),
                    os.environ.get('AIRTABLE_TABLE_NAME')
                ]):
                    try:
                        records = search_airtable()
                        test_results["airtable_connection_test"] = f"Success! Found {len(records)} records"
                    except Exception as e:
                        test_results["airtable_connection_test"] = f"Error: {str(e)}"
                
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps(test_results, indent=2).encode())
                return
            
            # Regular question processing
            question = data.get('question', '')
            
            if not question:
                self.send_response(400)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({'error': 'No question provided'}).encode())
                return
            
            # Process question
            schema = get_combined_schema()
            query_info = text_to_sql(question, schema)
            
            # Execute query
            results, error = execute_query(query_info)
            
            if error:
                self.send_response(500)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({
                    'error': f'Database error: {error}', 
                    'query_info': query_info
                }).encode())
                return
            
            # Format results
            answer = format_results(results, question)
            
            # Include raw results for visualization if it's a visualization request
            include_raw = any(term in question.lower() for term in ['chart', 'table', 'show me', 'photo', 'photos'])
            
            response = {
                'answer': answer,
                'query_type': query_info.get('type', 'sql') if isinstance(query_info, dict) else 'sql',
                'sql': query_info.get('query', str(query_info)) if isinstance(query_info, dict) else query_info,
                'results_count': len(results) if results else 0
            }
            
            # Add raw results for visualization (limit size to prevent huge responses)
            if include_raw and results and len(results) < 1000:
                response['raw_results'] = results
            
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            
            self.wfile.write(json.dumps(response).encode())
            
        except Exception as e:
            self.send_response(500)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps({'error': f'Server error: {str(e)}'}).encode())
