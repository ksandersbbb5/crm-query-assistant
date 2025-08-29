from http.server import BaseHTTPRequestHandler
import openai
import os
import json
import decimal
import datetime

# Try to import pyodbc for SQL Server connection
try:
    import pyodbc
    PYODBC_AVAILABLE = True
except ImportError:
    PYODBC_AVAILABLE = False
    print("pyodbc not available - using mock data only")

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

def get_db_connection():
    """Create SQL Server connection"""
    if not PYODBC_AVAILABLE:
        return None
        
    # Check if we have all required environment variables
    required_vars = ['SQL_SERVER', 'SQL_DATABASE', 'SQL_USERNAME', 'SQL_PASSWORD']
    if not all(os.environ.get(var) for var in required_vars):
        print("Missing required SQL Server environment variables")
        return None
    
    try:
        connection_string = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={os.environ.get('SQL_SERVER')};"
            f"DATABASE={os.environ.get('SQL_DATABASE')};"
            f"UID={os.environ.get('SQL_USERNAME')};"
            f"PWD={os.environ.get('SQL_PASSWORD')};"
            f"TrustServerCertificate=yes;"
        )
        return pyodbc.connect(connection_string)
    except Exception as e:
        print(f"SQL Server connection error: {str(e)}")
        return None

def text_to_sql(question, schema):
    """Convert question to SQL"""
    try:
        # For testing with mock data, use a dummy key if none is set
        if not openai.api_key:
            # Return a simple SQL query for mock testing
            if "45874" in question:
                return "SELECT * FROM Applications WHERE AppID = 45874"
            elif "count" in question.lower():
                return "SELECT COUNT(*) as count FROM Applications"
            elif "average" in question.lower() and "invoice" in question.lower():
                return "SELECT rep, AVG(invoice_total) as avg_invoice FROM Applications GROUP BY rep ORDER BY avg_invoice DESC"
            elif "rejected" in question.lower() or "denied" in question.lower():
                return "SELECT * FROM Applications WHERE app_status IN ('Rejected/Denied') ORDER BY DateCreated DESC"
            elif "balance" in question.lower():
                return "SELECT * FROM Applications WHERE invoice_balance > 0 ORDER BY invoice_balance DESC"
            elif "top" in question.lower() and "cities" in question.lower():
                return "SELECT TOP 10 city, COUNT(*) as count FROM Applications GROUP BY city ORDER BY count DESC"
            else:
                return "SELECT TOP 5 * FROM Applications ORDER BY DateCreated DESC"
        
        response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[
                {
                    "role": "system",
                    "content": f"""Convert natural language to SQL Server queries for Azure SQL Database.
                    
{schema}

Rules:
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
- Use single quotes for string values
- Be careful with date formatting"""
                },
                {"role": "user", "content": question}
            ],
            temperature=0.1,
            max_tokens=500
        )
        
        sql = response.choices[0].message.content.strip()
        
        # Clean up the response (remove markdown formatting if present)
        if sql.startswith('```sql'):
            sql = sql[6:-3]
        elif sql.startswith('```'):
            sql = sql[3:-3]
            
        return sql.strip()
        
    except Exception as e:
        # Return a default query for testing
        return "SELECT TOP 5 * FROM Applications ORDER BY DateCreated DESC"

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
                    "rep": "Cochrane, Valerie"
                }
            ]
        elif "COUNT" in sql_query.upper():
            mock_results = [{"count": 20000}]
        elif "TOP" in sql_query.upper() and "city" in sql_query.lower():
            mock_results = [
                {"city": "Boston", "count": 2500},
                {"city": "Cambridge", "count": 1800},
                {"city": "Worcester", "count": 1200},
                {"city": "Springfield", "count": 800},
                {"city": "Lowell", "count": 600}
            ]
        elif "rep" in sql_query.lower() and ("AVG" in sql_query.upper() or "average" in sql_query.lower()):
            mock_results = [
                {"rep": "Cochrane, Valerie", "avg_invoice": 625.50},
                {"rep": "DeLuca, MaryJane", "avg_invoice": 615.75},
                {"rep": "Padula, Denise", "avg_invoice": 595.25},
                {"rep": "Better Business Bureau, Online", "avg_invoice": 542.80}
            ]
        elif "rejected" in sql_query.lower() or "denied" in sql_query.lower():
            mock_results = [
                {
                    "AppID": 8783,
                    "app_status": "Rejected/Denied",
                    "dba": "Chiropractic Solutions, LLC",
                    "city": "Framingham",
                    "state": "MA",
                    "rep": "Weinstein, Sheryl"
                },
                {
                    "AppID": 721,
                    "app_status": "Rejected/Denied", 
                    "dba": "P&L Limousine",
                    "city": "Dorchester",
                    "state": "MA",
                    "rep": "Better Business Bureau, Better"
                }
            ]
        elif "balance" in sql_query.lower() and ">" in sql_query:
            mock_results = [
                {
                    "AppID": 83068,
                    "dba": "FDS Installers Inc",
                    "city": "Wareham",
                    "state": "MA",
                    "invoice_balance": 581,
                    "rep": "IABBB, Online"
                },
                {
                    "AppID": 78763,
                    "dba": "0136673821",
                    "city": "New Bedford",
                    "state": "MA",
                    "invoice_balance": 281,
                    "rep": "Better Business Bureau, Online"
                }
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
                    "rep": "Sample Rep"
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
    print(f"PYODBC Available: {PYODBC_AVAILABLE}")
    
    # Try real SQL Server connection first
    conn = get_db_connection()
    
    if conn:
        print("Successfully connected to SQL Server!")
        try:
            cursor = conn.cursor()
            
            # Execute the query
            cursor.execute(sql_query)
            
            # Get column names
            columns = [column[0] for column in cursor.description] if cursor.description else []
            
            # Fetch all results
            results = []
            for row in cursor.fetchall():
                result_dict = {}
                for i, column in enumerate(columns):
                    value = row[i]
                    # Handle different data types
                    if isinstance(value, decimal.Decimal):
                        value = float(value)
                    elif isinstance(value, datetime.datetime):
                        value = value.isoformat()
                    elif isinstance(value, datetime.date):
                        value = value.isoformat()
                    result_dict[column] = value
                results.append(result_dict)
            
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

def format_results(results, question):
    """Use LLM to format results into natural language answer"""
    if not results:
        return "No results found for your question."
    
    try:
        # For testing without OpenAI key
        if not openai.api_key:
            if len(results) == 1:
                # Format single result nicely
                if 'count' in results[0]:
                    return f"The count is: {results[0]['count']:,}"
                else:
                    formatted = "Found 1 result:\n"
                    for key, value in results[0].items():
                        formatted += f"- {key}: {value}\n"
                    return formatted.strip()
            else:
                formatted = f"Found {len(results)} results:\n\n"
                for i, result in enumerate(results[:3], 1):
                    formatted += f"Result {i}:\n"
                    for key, value in result.items():
                        formatted += f"- {key}: {value}\n"
                    formatted += "\n"
                if len(results) > 3:
                    formatted += f"... and {len(results) - 3} more results"
                return formatted.strip()
        
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
                    "content": "Format database query results into a clear, natural language answer. Be concise but informative."
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
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()

    def do_GET(self):
        """Handle GET requests for testing"""
        try:
            if self.path == '/api/query/test':
                # Test database connection
                test_results = {
                    "pyodbc_available": PYODBC_AVAILABLE,
                    "sql_server": os.environ.get('SQL_SERVER', 'Not Set'),
                    "sql_database": os.environ.get('SQL_DATABASE', 'Not Set'),
                    "sql_username": os.environ.get('SQL_USERNAME', 'Not Set'),
                    "sql_password": "***" if os.environ.get('SQL_PASSWORD') else "Not Set",
                    "openai_key": "Set" if os.environ.get('OPENAI_API_KEY') else "Not Set",
                    "connection_test": "Not Tested"
                }
                
                # Try to connect
                if PYODBC_AVAILABLE and all([
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
                            test_results["connection_test"] = f"Success! Found {result[0]} records"
                            cursor.close()
                            conn.close()
                        else:
                            test_results["connection_test"] = "Failed to create connection"
                    except Exception as e:
                        test_results["connection_test"] = f"Error: {str(e)}"
                else:
                    test_results["connection_test"] = "Missing configuration"
                
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps(test_results, indent=2).encode())
                return
                
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
            
            question = data.get('question', '')
            
            if not question:
                self.send_response(400)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({'error': 'No question provided'}).encode())
                return
            
            # Process question
            schema = get_database_schema()
            sql_query = text_to_sql(question, schema)
            
            if sql_query.startswith('Error'):
                self.send_response(500)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({'error': sql_query}).encode())
                return
            
            results, error = execute_sql_via_rest(sql_query)
            
            if error:
                self.send_response(500)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({'error': f'Database error: {error}', 'sql': sql_query}).encode())
                return
            
            answer = format_results(results, question)
            
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            
            response = {
                'answer': answer,
                'sql': sql_query,
                'results_count': len(results) if results else 0
            }
            
            self.wfile.write(json.dumps(response).encode())
            
        except Exception as e:
            self.send_response(500)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps({'error': f'Server error: {str(e)}'}).encode())
