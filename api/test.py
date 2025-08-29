from http.server import BaseHTTPRequestHandler
import os
import json

try:
    import pyodbc
    PYODBC_AVAILABLE = True
except ImportError:
    PYODBC_AVAILABLE = False

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        """Handle GET requests for testing"""
        try:
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
                    connection_string = (
                        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
                        f"SERVER={os.environ.get('SQL_SERVER')};"
                        f"DATABASE={os.environ.get('SQL_DATABASE')};"
                        f"UID={os.environ.get('SQL_USERNAME')};"
                        f"PWD={os.environ.get('SQL_PASSWORD')};"
                        f"TrustServerCertificate=yes;"
                    )
                    conn = pyodbc.connect(connection_string)
                    cursor = conn.cursor()
                    cursor.execute("SELECT COUNT(*) as count FROM Applications")
                    result = cursor.fetchone()
                    test_results["connection_test"] = f"Success! Found {result[0]} records"
                    cursor.close()
                    conn.close()
                except Exception as e:
                    test_results["connection_test"] = f"Error: {str(e)}"
            else:
                test_results["connection_test"] = "Missing configuration"
            
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
