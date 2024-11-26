import yaml
import psycopg2
from typing import Dict, List
import json

def get_column_info(cursor, table_name: str) -> Dict:
    """Get column information for a specific table"""
    cursor.execute("""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = %s
    """, (table_name,))
    
    columns = {}
    for col in cursor.fetchall():
        columns[col[0]] = col[1]
    return columns

def generate_db_config() -> Dict:
    """Generate database configuration by connecting to databases"""
    
    # Initialize the config structure
    config = {"databases": []}
    
    # Database configurations
    dbs = [
        {
            "name": "DB1",
            "host": "localhost",
            "port": "5432",
            "dbname": "arkaidDB",
            "username": "arkaid",
            "password": "arkaid"
        },
        {
            "name": "DB2",
            "host": "arkaid-postgres.crcw0q6uux8e.ap-south-1.rds.amazonaws.com",
            "port": "5432",
            "dbname": "postgres",
            "username": "postgres",
            "password": "postgres"
        }
    ]
    
    for db in dbs:
        try:
            # Connect to database
            conn = psycopg2.connect(
                host=db["host"],
                port=db["port"],
                dbname=db["dbname"],
                user=db["username"],
                password=db["password"]
            )
            
            cursor = conn.cursor()
            
            # Get all tables in the database
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
            """)
            
            tables = []
            for table in cursor.fetchall():
                table_name = table[0]
                columns = get_column_info(cursor, table_name)
                tables.append({
                    "name": table_name,
                    "columns": columns
                })
            
            # Add database config with tables
            db_config = {
                "name": db["name"],
                "host": db["host"],
                "port": db["port"],
                "dbname": db["dbname"],
                "username": db["username"],
                "password": db["password"],
                "tables": tables
            }
            
            config["databases"].append(db_config)
            
            cursor.close()
            conn.close()
            
        except Exception as e:
            print(f"Error connecting to {db['name']}: {str(e)}")
            continue
    
    return config

def save_config(config: Dict, filename: str = "config.yaml"):
    """Save configuration to YAML file"""
    with open(filename, 'w') as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False)

def main():
    config = generate_db_config()
    save_config(config)
    print("Database configuration has been generated successfully!")

if __name__ == "__main__":
    main() 