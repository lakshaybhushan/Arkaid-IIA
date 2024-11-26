import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import os
from typing import List

def get_csv_files(directory: str) -> List[str]:
    """Get all CSV files from the specified directory"""
    csv_files = []
    for file in os.listdir(directory):
        if file.endswith('.csv'):
            csv_files.append(os.path.join(directory, file))
    return csv_files

def create_postgres_table(csv_path: str, db_params: dict):
    """Create PostgreSQL table from CSV file"""
    try:
        # Read CSV file
        df = pd.read_csv(csv_path)
        
        # Convert all column names to lowercase
        df.columns = df.columns.str.lower()
        
        # Create table name from file name (remove extension and special characters)
        table_name = os.path.basename(csv_path).replace('.csv', '').replace(' ', '_').lower()
        
        # Create SQLAlchemy engine
        engine = create_engine(
            f"postgresql://{db_params['user']}:{db_params['password']}@"
            f"{db_params['host']}:{db_params['port']}/{db_params['database']}"
        )
        
        # Create table and insert data
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"Successfully created table: {table_name}")
        
    except Exception as e:
        print(f"Error processing {csv_path}: {str(e)}")

def main():
    # Database connection parameters
    db_params = {
        'host': 'localhost',
        'database': 'arkaidDB',
        'user': 'arkaid',
        'password': 'arkaid',
        'port': '5432'
    }
    
    # Directory containing CSV files
    csv_directory = './de'
    
    # Get all CSV files
    csv_files = get_csv_files(csv_directory)
    
    if not csv_files:
        print("No CSV files found in the specified directory")
        return
    
    # Process each CSV file
    for csv_file in csv_files:
        create_postgres_table(csv_file, db_params)

if __name__ == "__main__":
    main()