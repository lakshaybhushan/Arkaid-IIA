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
    if 'release_date' in df.columns:
        df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce').dt.strftime('%Y-%m-%d')

def create_table_query(df: pd.DataFrame, table_name: str) -> str:
    """Generate CREATE TABLE query based on DataFrame schema"""
    column_types = {
        'id': 'TEXT PRIMARY KEY',
        'name': 'TEXT',
        'release_date': 'DATE',
        'age_rating': 'TEXT',
        'price': 'FLOAT',
        'dlc_count': 'INTEGER',
        'about': 'TEXT',
        'languages': 'TEXT',
        'developer': 'TEXT',
        'publisher': 'TEXT',
        'platforms': 'TEXT',
        'categories': 'TEXT',
        'genres': 'TEXT',
        'tags': 'TEXT',
        'achievements': 'INTEGER',
        'positive_ratings': 'INTEGER',
        'negative_ratings': 'INTEGER',
        'average_playtime': 'FLOAT',
        'median_playtime': 'FLOAT',
        'owners': 'TEXT',
        'average_hours_played': 'FLOAT'
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
        print("Reading steam_games.csv...")
        df = pd.read_csv(
            'data/steam_games.csv',
            dtype={
                'id': 'str',
                'name': 'str',
                'age_rating': 'str',
                'price': 'float64',
                'dlc_count': 'Int64',
                'achievements': 'Int64',
                'positive_ratings': 'Int64',
                'negative_ratings': 'Int64',
                'average_playtime': 'float64',
                'median_playtime': 'float64',
                'average_hours_played': 'float64'
            },
            engine='c',
            low_memory=False
        )
        
        # Transform column names (vectorized operation)
        print("Transforming column names...")
        df.columns = [convert_to_snake_case(col) for col in df.columns]
        
        # Rename required_age to age_rating
        print("Renaming required_age to age_rating...")
        df = df.rename(columns={'required_age': 'age_rating'})
        
        # Remove duplicates (inplace operation)
        print("Removing duplicate entries...")
        df.drop_duplicates(subset=['id'], inplace=True, keep='first')
        
        # Format dates
        print("Processing dates...")
        format_date_columns(df)
        
        # Create table with index
        table_name = 'steam_games'
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