import requests
import openai
import os
import json
import urllib.parse
import base64

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

def text_to_sql(question, schema):
    """Convert question to SQL"""
    try:
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
        return f"Error generating SQL: {str(e)}"

def execute_sql_via_rest(sql_query):
    """Execute SQL query using Azure SQL Database REST API"""
    try:
        # Azure SQL Database REST API endpoint
        server = os.environ.get('AZURE_SQL_SERVER')
        database = os.environ.get('AZURE_SQL_DATABASE')
        username = os.environ.get('AZURE_SQL_USERNAME')
        password = os.environ.get('AZURE_SQL_PASSWORD')
        
        # Create connection string for REST API
        connection_string = f"Server=tcp:{server},1433;Initial Catalog={database};Persist Security Info=False;User ID={username};Password={password};MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;"
        
        # Use Azure Data Studio REST API approach
        # This is a simplified approach - in production you'd want to use Azure's official REST APIs
        
        # For now, let's create a mock response to test the system
        # In a real implementation, you would use Azure's official SQL REST API
        
        # Mock data for testing - replace with actual REST API call
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
        else:
            mock_results = [
                {
                    "AppID": 12345,
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

def format_results(results, question):
    """Use LLM to format results into natural language answer"""
    if not results:
        return "No results found for your question."
    
    try:
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
        return f"Results formatting error: {str(e)}"

def handler(request):
    """Main handler for Vercel serverless function"""
    try:
        # Handle CORS preflight
        if hasattr(request, 'method') and request.method == 'OPTIONS':
            return {
                'statusCode': 200,
                'headers': {
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'POST, OPTIONS',
                    'Access-Control-Allow-Headers': 'Content-Type',
                }
            }
        
        # Handle POST request
        if hasattr(request, 'method') and request.method == 'POST':
            # Get request body
            try:
                if hasattr(request, 'get_json'):
                    data = request.get_json()
                elif hasattr(request, 'json'):
                    data = request.json
                else:
                    import json
                    data = json.loads(request.body if hasattr(request, 'body') else '{}')
            except:
                data = {}
            
            question = data.get('question', '')
            
            if not question:
                return {
                    'statusCode': 400,
                    'headers': {'Access-Control-Allow-Origin': '*', 'Content-Type': 'application/json'},
                    'body': json.dumps({'error': 'No question provided'})
                }
            
            # Process question
            schema = get_database_schema()
            sql_query = text_to_sql(question, schema)
            
            if sql_query.startswith('Error'):
                return {
                    'statusCode': 500,
                    'headers': {'Access-Control-Allow-Origin': '*', 'Content-Type': 'application/json'},
                    'body': json.dumps({'error': sql_query})
                }
            
            results, error = execute_sql_via_rest(sql_query)
            
            if error:
                return {
                    'statusCode': 500,
                    'headers': {'Access-Control-Allow-Origin': '*', 'Content-Type': 'application/json'},
                    'body': json.dumps({'error': f'Database error: {error}', 'sql': sql_query})
                }
            
            answer = format_results(results, question)
            
            return {
                'statusCode': 200,
                'headers': {'Access-Control-Allow-Origin': '*', 'Content-Type': 'application/json'},
                'body': json.dumps({
                    'answer': answer,
                    'sql': sql_query,
                    'results_count': len(results) if results else 0
                })
            }
        
        return {
            'statusCode': 405,
            'headers': {'Access-Control-Allow-Origin': '*', 'Content-Type': 'application/json'},
            'body': json.dumps({'error': 'Method not allowed'})
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'headers': {'Access-Control-Allow-Origin': '*', 'Content-Type': 'application/json'},
            'body': json.dumps({'error': f'Server error: {str(e)}'})
        }
