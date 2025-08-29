from http.server import BaseHTTPRequestHandler
import os
import json

try:
    import pymssql
    MSSQL_AVAILABLE = True
except ImportError:
    MSSQL_AVAILABLE = False

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        """Handle GET requests for testing"""
        try:
            # Test database connection
            test_results = {
                "pymssql_available": MSSQL_AVAILABLE,
                "sql_server": os.environ.get('SQL_SERVER', 'Not Set'),
                "sql_database": os.environ.get('SQL_DATABASE', 'Not Set'),
                "sql_username": os.environ.get('SQL_USERNAME', 'Not Set'),
                "sql_password": "***" if os.environ.get('SQL_PASSWORD') else "Not Set",
                "openai_key": "Set" if os.environ.get('OPENAI_API_KEY') else "Not Set",
                "connection_test": "Not Tested"
            }
            
            # Try to connect
            if MSSQL_AVAILABLE and all([
                os.environ.get('SQL_SERVER'),
                os.environ.get('SQL_DATABASE'),
                os.environ.get('SQL_USERNAME'),
                os.environ.get('SQL_PASSWORD')
            ]):
                try:
                    conn = pymssql.connect(
                        server=os.environ.get('SQL_SERVER'),
                        database=os.environ.get('SQL_DATABASE'),
                        user=os.environ.get('SQL_USERNAME'),
                        password=os.environ.get('SQL_PASSWORD'),
                        as_dict=True
                    )
                    cursor = conn.cursor()
                    cursor.execute("SELECT COUNT(*) as count FROM Applications")
                    result = cursor.fetchone()
                    test_results["connection_test"] = f"Success! Found {result['count']} records in Applications table"
                    
                    # Get a sample record to verify structure
                    cursor.execute("SELECT TOP 1 AppID, app_status, dba, city, state FROM Applications")
                    sample = cursor.fetchone()
                    if sample:
                        test_results["sample_record"] = {
                            "AppID": sample.get('AppID'),
                            "app_status": sample.get('app_status'),
                            "dba": sample.get('dba'),
                            "city": sample.get('city'),
                            "state": sample.get('state')
                        }
                    
                    cursor.close()
                    conn.close()
                except Exception as e:
                    test_results["connection_test"] = f"Error: {str(e)}"
                    test_results["error_type"] = type(e).__name__
            else:
                if not MSSQL_AVAILABLE:
                    test_results["connection_test"] = "pymssql not installed"
                else:
                    missing = []
                    if not os.environ.get('SQL_SERVER'): missing.append('SQL_SERVER')
                    if not os.environ.get('SQL_DATABASE'): missing.append('SQL_DATABASE')
                    if not os.environ.get('SQL_USERNAME'): missing.append('SQL_USERNAME')
                    if not os.environ.get('SQL_PASSWORD'): missing.append('SQL_PASSWORD')
                    test_results["connection_test"] = f"Missing environment variables: {', '.join(missing)}"
            
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps(test_results, indent=2).encode())
            
        except Exception as e:
            self.send_response(500)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps({'error': str(e)}).encode())

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()
