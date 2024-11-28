import yaml
import psycopg2
from typing import Dict, List

class DatabaseConnector:
    def __init__(self, config_path: str = 'db_config.yaml'):
        """Initialize DatabaseConnector with the path to config file"""
        self.config_path = config_path
        self.connections = {}
        self.load_config()
    
    def load_config(self) -> None:
        """Load database configurations from YAML file"""
        with open(self.config_path, 'r') as file:
            self.config = yaml.safe_load(file)
    
    def connect_to_databases(self) -> Dict[str, psycopg2.extensions.connection]:
        """Establish connections to all databases defined in config"""
        try:
            for db in self.config['databases']:
                conn = psycopg2.connect(
                    host=db['host'],
                    port=db['port'],
                    dbname=db['dbname'],
                    user=db['username'],
                    password=db['password']
                )
                self.connections[db['name']] = conn
                print(f"Successfully connected to {db['name']}")
                
            return self.connections
        
        except Exception as e:
            print(f"Error connecting to databases: {str(e)}")
            raise
    
    def close_connections(self) -> None:
        """Close all database connections"""
        for db_name, conn in self.connections.items():
            conn.close()
            print(f"Connection closed for {db_name}")

# Example usage
if __name__ == "__main__":
    # Initialize the connector
    db_connector = DatabaseConnector()
    
    try:
        # Connect to all databases
        connections = db_connector.connect_to_databases()
        
        # Now you can use the connections
        # Example: Get cursor for DB1
        cursor_db1 = connections['DB1'].cursor()
        
        # Example: Get cursor for DB2
        cursor_db2 = connections['DB2'].cursor()
        
        # Perform your database operations here
        # ...
        
    finally:
        # Always close connections when done
        db_connector.close_connections() 