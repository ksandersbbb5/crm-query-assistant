from flask import Flask, request, jsonify
import pyodbc
import openai
import os
import json

app = Flask(__name__)

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
        
        cursor.execute(schema_query)
        schema = cursor.fetchall()
        
        schema_text = "Database Schema:\n"
        current_table = ""
        
        for row in schema:
            table, column, data_type = row
            if table != current_table:
                current_table = table
                schema_text += f"\nTable: {table}\n"
            schema_text += f"  - {column} ({data_type})\n"
        
        conn.close()
        return schema_text
        
    except Exception as e:
        return f"Schema error: {str(e)}"

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
- Always include reasonable TOP limits for large result sets"""
                },
                {"role": "user", "content": question}
            ],
            temperature=0.1,
            max_tokens=500
        )
        
        sql = response.choices[0].message.content.strip()
        if sql.startswith('```'):
            sql = sql.split('\n')[1:-1]
            sql = '\n'.join(sql)
        
        return sql
        
    except Exception as e:
        return f"Error: {str(e)}"

def execute_sql(sql_query):
    """Execute SQL query"""
    try:
        conn = pyodbc.connect(os.environ.get('SQL_SERVER_CONNECTION_STRING'))
        cursor = conn.cursor()
        
        cursor.execute(sql_query)
        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchall()
        
        results = [dict(zip(columns, row)) for row in rows]
        
        conn.close()
        return results, None
        
    except Exception as e:
        return None, str(e)

def format_results(results, question):
    """Format results with LLM"""
    if not results:
        return "No results found."
    
    try:
        result_text = f"Found {len(results)} results:\n"
        for i, result in enumerate(results[:10]):
            result_text += f"{i+1}. {result}\n"
        
        response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "Format database results into clear natural language."},
                {"role": "user", "content": f"Question: {question}\n\nResults: {result_text}"}
            ],
            temperature=0.1,
            max_tokens=300
        )
        
        return response.choices[0].message.content.strip()
        
    except Exception as e:
        return f"Formatting error: {str(e)}"

def handler(request):
    if request.method == 'POST':
        try:
            data = request.get_json()
            question = data.get('question', '')
            
            if not question:
                return jsonify({'error': 'No question provided'})
            
            # Process question
            schema = get_database_schema()
            sql_query = text_to_sql(question, schema)
            
            if sql_query.startswith('Error'):
                return jsonify({'error': sql_query})
            
            results, error = execute_sql(sql_query)
            
            if error:
                return jsonify({'error': f'Database error: {error}', 'sql': sql_query})
            
            answer = format_results(results, question)
            
            return jsonify({
                'answer': answer,
                'sql': sql_query,
                'results_count': len(results) if results else 0
            })
            
        except Exception as e:
            return jsonify({'error': f'Server error: {str(e)}'})
    
    return jsonify({'error': 'Method not allowed'})
