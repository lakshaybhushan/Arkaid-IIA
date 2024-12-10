import pandas as pd
import re
from db_connection import DatabaseConnector
from typing import List, Dict
from datetime import datetime
import psycopg2.extras
import io
import numpy as np

def convert_to_snake_case(name: str) -> str:
    """Convert a column name to snake_case format and handle double underscores"""
    # First convert camelCase and PascalCase to snake_case
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    s2 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1)
    
    # Convert spaces, hyphens and other special chars to underscores
    s3 = re.sub(r'[-\s]+', '_', s2)
    
    # Convert to lowercase and remove any non-alphanumeric characters except underscores
    s4 = re.sub(r'[^\w]+', '', s3).lower()
    
    # Replace multiple underscores with a single underscore
    s5 = re.sub(r'_+', '_', s4)
    
    return s5.strip('_')

def create_table_query(df: pd.DataFrame, table_name: str) -> str:
    """Generate CREATE TABLE query based on DataFrame schema"""
    column_types = {
        'id': 'TEXT PRIMARY KEY',
        'name': 'TEXT',
        'country': 'TEXT',
        'type_of_content': 'TEXT',
        'average_views': 'INTEGER',
        'revenue': 'FLOAT',
        'primary_game': 'TEXT'
    }
    
    columns = [f"{column} {column_types.get(column.lower(), 'TEXT')}" 
              for column in df.columns]
    
    return f"""
    DROP TABLE IF EXISTS {table_name} CASCADE;
    CREATE TABLE {table_name} (
        {', '.join(columns)}
    );
    CREATE INDEX idx_{table_name}_id ON {table_name}(id);
    """

def batch_insert_data(conn, table_name: str, df: pd.DataFrame, batch_size: int = 10000) -> None:
    """Fast batch insert using copy_expert"""
    
    # Create a buffer
    output = io.StringIO()
    
    # Write the DataFrame to the buffer in CSV format
    df.to_csv(output, sep='\t', header=False, index=False, na_rep='\\N')
    output.seek(0)
    
    # Create a cursor with name to avoid conflicts
    cur = conn.cursor()
    
    try:
        # Use COPY command for faster insertion
        cur.copy_expert(f"""
            COPY {table_name} FROM STDIN WITH (
                FORMAT CSV,
                DELIMITER E'\\t',
                NULL '\\N'
            )
        """, output)
        
    finally:
        cur.close()

def main():
    # Initialize database connection
    db_connector = DatabaseConnector()
    
    try:
        # Connect to database
        connections = db_connector.connect_to_databases()
        db1_conn = connections['DB1']
        cursor = db1_conn.cursor()
        
        # Read CSV file with optimized settings
        print("Reading content_creators.csv...")
        df = pd.read_csv(
            'data/content_creators.csv',
            dtype={
                'id': 'str',
                'name': 'str',
                'country': 'str',
                'type_of_content': 'str',
                'average_views': 'Int64',
                'revenue': 'float64',
                'primary_game': 'str'
            },
            engine='c',
            low_memory=False
        )
        
        # Transform column names (vectorized operation)
        print("Transforming column names...")
        df.columns = [convert_to_snake_case(col) for col in df.columns]
        
        # Remove duplicates (inplace operation)
        print("Removing duplicate entries...")
        df.drop_duplicates(subset=['id'], inplace=True, keep='first')
        
        # Create table with index
        table_name = 'content_creators'
        print(f"Creating table {table_name}...")
        create_query = create_table_query(df, table_name)
        cursor.execute(create_query)
        
        # Batch insert data
        print("Inserting data in batches...")
        batch_insert_data(db1_conn, table_name, df)
        
        # Commit transaction
        db1_conn.commit()
        print("Data loaded successfully!")
        
        # Print summary
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = cursor.fetchone()[0]
        print(f"Total rows inserted: {row_count}")
        
    except Exception as e:
        print(f"Error during ETL process: {str(e)}")
        if 'db1_conn' in locals():
            db1_conn.rollback()
    
    finally:
        db_connector.close_connections()

if __name__ == "__main__":
    main() 