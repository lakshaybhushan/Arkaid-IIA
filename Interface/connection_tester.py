import psycopg2
import yaml
from typing import Dict, Any
import sys
from rich.console import Console
from rich.table import Table

class DatabaseConnectionTester:
    def __init__(self, yaml_file_path: str):
        self.console = Console()
        self.db_config = self._load_yaml_config(yaml_file_path)
        
    def _load_yaml_config(self, file_path: str) -> Dict[str, Any]:
        try:
            with open(file_path, 'r') as file:
                return yaml.safe_load(file)
        except FileNotFoundError:
            self.console.print(f"[red]Error: Config file {file_path} not found!")
            sys.exit(1)
        except yaml.YAMLError as e:
            self.console.print(f"[red]Error parsing YAML file: {e}")
            sys.exit(1)
    
    def test_connection(self, db_config: Dict[str, str]) -> bool:
        try:
            conn = psycopg2.connect(
                host=db_config['host'],
                port=db_config['port'],
                dbname=db_config['dbname'],
                user=db_config['username'],
                password=db_config['password']
            )
            
            # Test if we can execute a simple query
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                
            conn.close()
            return True
            
        except Exception as e:
            return False, str(e)
    
    def test_table_existence(self, db_config: Dict[str, str], table_name: str) -> bool:
        try:
            conn = psycopg2.connect(
                host=db_config['host'],
                port=db_config['port'],
                dbname=db_config['dbname'],
                user=db_config['username'],
                password=db_config['password']
            )
            
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = %s
                    )
                """, (table_name,))
                exists = cur.fetchone()[0]
                
            conn.close()
            return exists
            
        except Exception as e:
            return False
    
    def run_tests(self):
        table = Table(title="Database Connection Test Results")
        table.add_column("Database", style="cyan")
        table.add_column("Host", style="magenta")
        table.add_column("Connection Status", style="green")
        table.add_column("Tables Status", style="yellow")
        
        for db in self.db_config['databases']:
            connection_result = self.test_connection(db)
            
            # Test tables if connection successful
            table_status = []
            if isinstance(connection_result, tuple):
                connection_status = f"[red]Failed: {connection_result[1]}"
                tables_status = "[red]Not tested - Connection failed"
            else:
                connection_status = "[green]Success"
                for table_info in db['tables']:
                    table_name = table_info['name']
                    exists = self.test_table_existence(db, table_name)
                    table_status.append(
                        f"{table_name}: [green]✓" if exists else f"{table_name}: [red]✗"
                    )
                tables_status = "\n".join(table_status)
            
            table.add_row(
                db['name'],
                db['host'],
                connection_status,
                tables_status
            )
        
        self.console.print(table)

def main():
    tester = DatabaseConnectionTester('db_config.yaml')
    tester.run_tests()

if __name__ == "__main__":
    main() 