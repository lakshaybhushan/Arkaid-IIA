import json
import yaml
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import Dict, List, Any
import hashlib

class WarehouseManager:
    def __init__(self, db_config_path: str, mapping_file_path: str, schema_mapping_path: str):
        # Load configurations
        self.db_config = self._load_yaml(db_config_path)
        self.mapping_config = self._load_json(mapping_file_path)
        self.schema_mappings = self._load_json(schema_mapping_path)
        
        # Get warehouse connection (DB3)
        self.warehouse_conn = self._get_warehouse_connection()
        
    def _load_yaml(self, path: str) -> dict:
        with open(path, 'r') as file:
            return yaml.safe_load(file)
            
    def _load_json(self, path: str) -> dict:
        with open(path, 'r') as file:
            return json.load(file)
    
    def _get_db_connection(self, db_name: str):
        """Create database connection based on database name"""
        db_info = next(db for db in self.db_config["databases"] 
                      if db["name"] == db_name)
        
        return psycopg2.connect(
            host=db_info["host"],
            port=db_info["port"],
            dbname=db_info["dbname"],
            user=db_info["username"],
            password=db_info["password"]
        )
    
    def _get_warehouse_connection(self):
        """Get connection to the warehouse database (DB3)"""
        return self._get_db_connection("DB3")
    
    def _get_postgres_type(self, type_str: str) -> str:
        """Convert generic type to PostgreSQL type"""
        type_mapping = {
            "AUTO_INCREMENT PRIMARY KEY": "UUID PRIMARY KEY",
            "Text": "TEXT",
            "Date": "DATE",
            "Float": "NUMERIC(10,2)",
            "Integer": "INTEGER",
            "Boolean": "BOOLEAN"
        }
        return type_mapping.get(type_str, type_str)
    
    def _get_schema_hash(self) -> str:
        """Generate a hash of the current schema and mappings configuration"""
        config_str = json.dumps({
            'target_schema': self.mapping_config['target_schema'],
            'mappings': self.schema_mappings
        }, sort_keys=True)
        return hashlib.md5(config_str.encode()).hexdigest()
    
    def _create_metadata_table(self):
        """Create metadata table if it doesn't exist"""
        cursor = self.warehouse_conn.cursor()
        try:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS materialized_view_metadata (
                    view_name TEXT PRIMARY KEY,
                    schema_hash TEXT,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            self.warehouse_conn.commit()
        finally:
            cursor.close()
    
    def _get_stored_schema_hash(self) -> str:
        """Retrieve the stored schema hash"""
        cursor = self.warehouse_conn.cursor()
        try:
            # Ensure metadata table exists
            self._create_metadata_table()
            
            cursor.execute("""
                SELECT schema_hash 
                FROM materialized_view_metadata 
                WHERE view_name = %s
            """, (self.mapping_config['materialized_view'],))
            
            result = cursor.fetchone()
            return result[0] if result else None
        finally:
            cursor.close()
    
    def _store_schema_hash(self, schema_hash: str):
        """Store the schema hash in a metadata table"""
        cursor = self.warehouse_conn.cursor()
        try:
            # Ensure metadata table exists
            self._create_metadata_table()
            
            # Store or update the hash
            cursor.execute("""
                INSERT INTO materialized_view_metadata (view_name, schema_hash)
                VALUES (%s, %s)
                ON CONFLICT (view_name) 
                DO UPDATE SET 
                    schema_hash = EXCLUDED.schema_hash,
                    last_updated = CURRENT_TIMESTAMP
            """, (self.mapping_config['materialized_view'], schema_hash))
            
            self.warehouse_conn.commit()
        finally:
            cursor.close()
    
    def _check_view_exists(self) -> bool:
        """Check if materialized view exists"""
        cursor = self.warehouse_conn.cursor()
        try:
            cursor.execute("""
                SELECT 1 
                FROM pg_matviews 
                WHERE matviewname = %s
            """, (self.mapping_config['materialized_view'],))
            
            return cursor.fetchone() is not None
        finally:
            cursor.close()
    
    def create_materialized_view(self):
        """Create or update the materialized view based on schema changes"""
        current_hash = self._get_schema_hash()
        stored_hash = self._get_stored_schema_hash()
        view_exists = self._check_view_exists()
        
        cursor = self.warehouse_conn.cursor()
        
        try:
            if not view_exists:
                print(f"Creating new materialized view {self.mapping_config['materialized_view']}...")
                self._create_new_view(cursor)
                
            elif current_hash != stored_hash:
                print("Schema changes detected. Recreating materialized view...")
                cursor.execute(f"DROP MATERIALIZED VIEW {self.mapping_config['materialized_view']}")
                self._create_new_view(cursor)
                
            else:
                print("No schema changes detected. Refreshing materialized view...")
                cursor.execute(f"REFRESH MATERIALIZED VIEW {self.mapping_config['materialized_view']}")
            
            self._store_schema_hash(current_hash)
            self.warehouse_conn.commit()
            print(f"Successfully updated materialized view {self.mapping_config['materialized_view']}")
            
        except Exception as e:
            self.warehouse_conn.rollback()
            print(f"Error managing materialized view: {str(e)}")
            raise
        finally:
            cursor.close()
    
    def _create_new_view(self, cursor):
        """Create a new materialized view"""
        # Create foreign server and user mappings if they don't exist
        self._setup_foreign_servers()
        
        # Create source queries
        source_queries = []
        
        for source in self.mapping_config["sources"]:
            column_mappings = self.schema_mappings[source["name"]]
            select_columns = []
            
            # Add UUID generation for game_id at the start
            select_columns.append("gen_random_uuid() as game_id")
            
            # Get target columns in the order they appear in target_schema
            target_columns = [
                col for col in self.mapping_config["target_schema"].keys() 
                if col != 'game_id'  # Skip game_id as we're generating it
            ]
            
            # For each target column, find its corresponding source column from the mappings
            for target_col in target_columns:
                # Find source column that maps to this target column
                source_col = None
                for src_col, tgt_col in column_mappings.items():
                    if tgt_col == target_col:
                        source_col = src_col
                        break
                
                if source_col:
                    target_type = self._get_postgres_type(self.mapping_config["target_schema"][target_col])
                    select_columns.append(f"CAST({source_col} AS {target_type}) as {target_col}")
                else:
                    # If no mapping found, use NULL with correct type
                    target_type = self._get_postgres_type(self.mapping_config["target_schema"][target_col])
                    select_columns.append(f"NULL::{target_type} as {target_col}")
            
            # Add hardcoded game_platform column based on source
            platform_name = 'Steam' if source["name"] == 'steam_games' else 'Epic'
            select_columns.append(f"'{platform_name}'::TEXT as game_platform")
            
            # Use schema-qualified foreign table reference
            query = f"""
                SELECT 
                    {', '.join(select_columns)}
                FROM {source["database"].lower()}.{source["name"]}
            """
            
            # Debug print
            print(f"\nQuery for {source['name']}:")
            print(query)
            
            source_queries.append(query)
        
        # Combine source queries with UNION ALL
        combined_query = " UNION ALL ".join(source_queries)
        
        # Create materialized view
        create_view_sql = f"""
        CREATE MATERIALIZED VIEW {self.mapping_config['materialized_view']} AS
        WITH combined_data AS (
            {combined_query}
        )
        SELECT * FROM combined_data;
        """
        
        # First ensure UUID extension is available
        cursor.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto;")
        
        # Create the view
        cursor.execute(create_view_sql)
        
        # Create a unique index on game_id
        cursor.execute(f"""
        CREATE UNIQUE INDEX {self.mapping_config['materialized_view']}_pkey 
        ON {self.mapping_config['materialized_view']} (game_id);
        """)
    
    def _setup_foreign_servers(self):
        """Setup foreign servers and user mappings for cross-database queries"""
        cursor = self.warehouse_conn.cursor()
        
        try:
            # Create postgres_fdw extension if it doesn't exist
            cursor.execute("CREATE EXTENSION IF NOT EXISTS postgres_fdw;")
            
            # Setup foreign servers for each source database
            for source in self.mapping_config["sources"]:
                db_info = next(db for db in self.db_config["databases"] 
                              if db["name"] == source["database"])
                
                # Create schema if it doesn't exist
                cursor.execute(f"""
                    CREATE SCHEMA IF NOT EXISTS {source["database"].lower()};
                """)
                
                # Create foreign server
                cursor.execute(f"""
                    DO $$
                    BEGIN
                        IF NOT EXISTS (
                            SELECT 1 FROM pg_foreign_server WHERE srvname = '{source["database"].lower()}'
                        ) THEN
                            CREATE SERVER {source["database"].lower()}
                            FOREIGN DATA WRAPPER postgres_fdw
                            OPTIONS (
                                host '{db_info["host"]}',
                                port '{db_info["port"]}',
                                dbname '{db_info["dbname"]}'
                            );
                        END IF;
                    END
                    $$;
                """)
                
                # Create user mapping
                cursor.execute(f"""
                    DO $$
                    BEGIN
                        IF NOT EXISTS (
                            SELECT 1 FROM pg_user_mappings 
                            WHERE srvname = '{source["database"].lower()}'
                            AND usename = current_user
                        ) THEN
                            CREATE USER MAPPING IF NOT EXISTS FOR current_user
                            SERVER {source["database"].lower()}
                            OPTIONS (
                                user '{db_info["username"]}',
                                password '{db_info["password"]}'
                            );
                        END IF;
                    END
                    $$;
                """)
                
                # Drop existing foreign table if exists
                cursor.execute(f"""
                    DROP FOREIGN TABLE IF EXISTS {source["database"].lower()}.{source["name"]};
                """)
                
                # Create foreign table
                cursor.execute(f"""
                    IMPORT FOREIGN SCHEMA public 
                    LIMIT TO ({source["name"]})
                    FROM SERVER {source["database"].lower()}
                    INTO {source["database"].lower()};
                """)
            
            self.warehouse_conn.commit()
            print("Successfully set up foreign servers and mappings")
            
        except Exception as e:
            self.warehouse_conn.rollback()
            print(f"Error setting up foreign servers: {str(e)}")
            raise
        finally:
            cursor.close()
    
    def close_connections(self):
        """Close warehouse connection"""
        if self.warehouse_conn:
            self.warehouse_conn.close()

if __name__ == "__main__":
    manager = WarehouseManager(
        db_config_path='db_config.yaml',
        mapping_file_path='mappings/mv_games.json',
        schema_mapping_path='mappings/mv_games_schema_mappings.json'
    )
    
    try:
        # Create or update materialized view
        manager.create_materialized_view()
        print("Successfully managed materialized view!")
    except Exception as e:
        print(f"Error: {str(e)}")
    finally:
        manager.close_connections() 