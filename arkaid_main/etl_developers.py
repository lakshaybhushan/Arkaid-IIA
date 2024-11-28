import pandas as pd
import re
from db_connection import DatabaseConnector
from typing import List, Dict
from datetime import datetime
import psycopg2.extras
import io
import numpy as np

def convert_to_snake_case(name: str) -> str:
    """Convert a column name to snake_case format"""
    return re.sub(r'[^\w]+', '_', 
                 re.sub('([a-z0-9])([A-Z])', r'\1_\2',
                       re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name))).lower().strip('_')

def format_date_columns(df: pd.DataFrame) -> None:
    """Convert date columns to YYYY-MM-DD format (in-place)"""
    if 'est' in df.columns:
        df['est'] = pd.to_datetime(df['est'], errors='coerce').dt.strftime('%Y-%m-%d')

def create_table_query(df: pd.DataFrame, table_name: str) -> str:
    """Generate CREATE TABLE query based on DataFrame schema"""
    # First create the base columns without the ID
    base_columns = {
        'developer': 'TEXT',
        'active': 'BOOLEAN',
        'city': 'TEXT',
        'country': 'TEXT',
        'est': 'DATE',
        'notable_games': 'TEXT',
        'notes': 'TEXT'
    }
    
    # Add developer_id as first column
    columns = [f"developer_id SERIAL PRIMARY KEY"] + \
             [f"{column} {base_columns.get(column.lower(), 'TEXT')}" 
              for column in df.columns]
    
    return f"""
    DROP TABLE IF EXISTS {table_name} CASCADE;
    CREATE TABLE {table_name} (
        {', '.join(columns)}
    );
    CREATE INDEX idx_{table_name}_developer_id ON {table_name}(developer_id);
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
            COPY {table_name} (developer, active, city, country, est, notable_games, notes) 
            FROM STDIN WITH (
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
        db2_conn = connections['DB2']
        cursor = db2_conn.cursor()
        
        # Read CSV file with optimized settings
        print("Reading developers.csv...")
        df = pd.read_csv(
            'data/developers.csv',
            dtype={
                'Developer': 'str',
                'Active': 'bool',
                'City': 'str',
                'Country': 'str',
                'Notes': 'str'
            },
            parse_dates=['Est.'],
            engine='c',
            low_memory=False
        )
        
        # First rename the problematic columns
        print("Renaming columns...")
        column_mapping = {
            'Developer': 'developer',
            'Active': 'active',
            'City': 'city',
            'Country': 'country',
            'Est.': 'est',
            'Notable games, series or franchises': 'notable_games',
            'Notes': 'notes'
        }
        
        # Rename columns using the mapping
        df = df.rename(columns=column_mapping)
        
        # Format dates
        print("Processing dates...")
        format_date_columns(df)
        
        # Create table with index
        table_name = 'developers'
        print(f"Creating table {table_name}...")
        create_query = create_table_query(df, table_name)
        cursor.execute(create_query)
        
        # Batch insert data
        print("Inserting data in batches...")
        batch_insert_data(db2_conn, table_name, df)
        
        # Commit transaction
        db2_conn.commit()
        print("Data loaded successfully!")
        
        # Print summary
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = cursor.fetchone()[0]
        print(f"Total rows inserted: {row_count}")
        
    except Exception as e:
        print(f"Error during ETL process: {str(e)}")
        if 'db2_conn' in locals():
            db2_conn.rollback()
    
    finally:
        db_connector.close_connections()

if __name__ == "__main__":
    main() 