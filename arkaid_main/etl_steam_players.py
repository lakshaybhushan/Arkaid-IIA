import pandas as pd
import re
from db_connection import DatabaseConnector
from typing import List, Dict
from datetime import datetime
import psycopg2.extras
from concurrent.futures import ThreadPoolExecutor
import io
import numpy as np

def convert_to_snake_case(name: str) -> str:
    """Convert a column name to snake_case format"""
    return re.sub(r'[^\w]+', '_', 
                 re.sub('([a-z0-9])([A-Z])', r'\1_\2',
                       re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name))).lower().strip('_')

def format_date_columns(df: pd.DataFrame) -> None:
    """Convert date columns to YYYY-MM-DD format (in-place)"""
    if 'dob' in df.columns:
        df['dob'] = pd.to_datetime(df['dob'], format='%Y-%m-%d', errors='coerce').dt.strftime('%Y-%m-%d')

def create_table_query(df: pd.DataFrame, table_name: str) -> str:
    """Generate CREATE TABLE query based on DataFrame schema"""
    column_types = {
        'id': 'TEXT PRIMARY KEY',
        'name': 'TEXT',
        'username': 'TEXT',
        'age': 'INTEGER',
        'dob': 'DATE',
        'email': 'TEXT',
        'phone': 'TEXT',
        'payment_method': 'TEXT',
        'country': 'TEXT',
        'total_hrs_played': 'FLOAT',
        'avg_rating_given': 'FLOAT'
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
        print("Reading steam_players.csv...")
        df = pd.read_csv(
            'data/steam_players.csv',
            dtype={
                'id': 'str',
                'age': 'Int64',
                'total_hrs_played': 'float64',
                'avg_rating_given': 'float64'
            },
            engine='c',  # Use C engine for faster reading
            low_memory=False
        )
        
        # Transform column names (vectorized operation)
        print("Transforming column names...")
        df.columns = [convert_to_snake_case(col) for col in df.columns]
        
        # Remove duplicates (inplace operation)
        print("Removing duplicate entries...")
        df.drop_duplicates(subset=['id'], inplace=True, keep='first')
        
        # Format dates
        print("Processing dates...")
        format_date_columns(df)
        
        # Create table with index
        table_name = 'steam_players'
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