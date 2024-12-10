import yaml
import psycopg2
from typing import Dict, List

def get_materialized_view_columns(cursor, view_name: str) -> Dict:
    """Get column information for a materialized view using PostgreSQL system catalogs"""
    cursor.execute("""
        SELECT a.attname as column_name,
               pg_catalog.format_type(a.atttypid, a.atttypmod) as data_type
        FROM pg_catalog.pg_attribute a
        JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
        JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
        WHERE c.relname = %s
        AND n.nspname = 'public'
        AND a.attnum > 0
        AND NOT a.attisdropped
        ORDER BY a.attnum;
    """, (view_name,))
    
    columns = {}
    for col in cursor.fetchall():
        columns[col[0]] = col[1]
    return columns

def get_materialized_views(cursor) -> List[str]:
    """Get list of materialized views"""
    cursor.execute("""
        SELECT matviewname 
        FROM pg_matviews
        WHERE schemaname = 'public'
    """)
    return [mv[0] for mv in cursor.fetchall()]

def generate_db_config() -> Dict:
    """Generate database configuration by connecting to databases"""
    
    # Initialize the config structure
    config = {"databases": []}
    
    # Database configurations
    dbs = [
        {
            "name": "DB1",
            "host": "dakshdb.crcw0q6uux8e.ap-south-1.rds.amazonaws.com",
            "port": "5432",
            "dbname": "postgres",
            "username": "dakshdb",
            "password": "dakshdb123"
        },
        {
            "name": "DB2",
            "host": "arkaid-postgres.crcw0q6uux8e.ap-south-1.rds.amazonaws.com",
            "port": "5432",
            "dbname": "postgres",
            "username": "postgres",
            "password": "postgres"
        },
        {
            "name": "DB3",
            "host": "centeralized-db.crcw0q6uux8e.ap-south-1.rds.amazonaws.com",
            "port": "5432",
            "dbname": "postgres",
            "username": "sameer123",
            "password": "?k$+M40waa1mWrs%BYtw3aorf#l3"
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
            tables = []
            
            if db["name"] == "DB3":
                # For DB3, only get materialized views
                materialized_views = get_materialized_views(cursor)
                for mv_name in materialized_views:
                    columns = get_materialized_view_columns(cursor, mv_name)
                    tables.append({
                        "name": mv_name,
                        "columns": columns,
                        "type": "materialized_view"
                    })
                    print(f"Found materialized view: {mv_name} with columns: {columns}")
            else:
                # For other DBs, get regular tables
                cursor.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_type = 'BASE TABLE'
                """)
                
                for table in cursor.fetchall():
                    table_name = table[0]
                    columns = get_materialized_view_columns(cursor, table_name)
                    tables.append({
                        "name": table_name,
                        "columns": columns,
                        "type": "table"
                    })
            
            # Add database config with tables/views
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
        yaml.dump(config, f, default_flow_style=False, sort_keys=False, indent=2)

def main():
    config = generate_db_config()
    save_config(config)
    print("Database configuration has been generated successfully!")

if __name__ == "__main__":
    main() 