from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from io import StringIO
import csv
import os

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

POSTGRES_CONN_ID = 'arkaid_aws_postgres'

# Function to process CSV file in chunks
def process_csv():
    # Define chunk size (adjust based on available memory)
    chunk_size = 100000
    
    # Process CSV in chunks
    chunks = pd.read_csv(
        'data_sources/developers.csv',
        chunksize=chunk_size,
        parse_dates=['Est.'],
        date_parser=lambda x: pd.to_datetime(x, format='%Y-%m-%d %H:%M:%S', errors='coerce')
    )
    
    # Process each chunk and save to temporary files
    for i, chunk in enumerate(chunks):

        chunk['Est.'] = pd.to_datetime(chunk['Est.'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
        
        chunk.to_csv(f'/tmp/processed_developers_chunk_{i}.csv', index=False)

def load_to_postgres():
    postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    
    # Create a connection
    conn = postgres_hook.get_conn()
    
    with conn.cursor() as cursor:
        cursor.execute("TRUNCATE TABLE developers;")
        
        cursor.execute("""
            SET work_mem = '1GB';
            SET maintenance_work_mem = '2GB';
            SET temp_buffers = '1GB';
        """)
    
    chunk_files = [f for f in os.listdir('/tmp') if f.startswith('processed_developers_chunk_')]
    
    for chunk_file in chunk_files:
        df = pd.read_csv(f'/tmp/{chunk_file}')
        
        buffer = StringIO()
        csv_writer = csv.writer(buffer, 
                              quoting=csv.QUOTE_MINIMAL,
                              escapechar='\\',
                              doublequote=True)
        
        # Write rows to buffer
        for _, row in df.iterrows():
            csv_writer.writerow([
                str(row['Developer']),
                int(row['Active']),
                str(row['City']),
                str(row['Autonomous area']),
                str(row['Country']),
                row['Est.'],
                str(row['Notable games, series or franchises']),
                str(row.get('Notes', ''))
            ])
        
        buffer.seek(0)
        
        # Use COPY command with CSV format specification and explicit column types
        with conn.cursor() as cursor:
            cursor.execute("""
                SET standard_conforming_strings = on;
                SET escape_string_warning = off;
            """)
            cursor.copy_expert(
                """
                COPY developers (
                    developer,
                    active,
                    city,
                    autonomous_area,
                    country,
                    established_date,
                    notable_games,
                    notes
                ) FROM STDIN WITH (
                    FORMAT CSV,
                    QUOTE '"',
                    ESCAPE '\\',
                    NULL '',
                    ENCODING 'UTF-8'
                )
                """,
                buffer
            )
        
        # Clean up the chunk file
        os.remove(f'/tmp/{chunk_file}')
    
    conn.commit()
    conn.close()

# Create DAG
with DAG(
    'etl_developer_pipeline',
    default_args=default_args,
    description='ETL pipeline for developers data',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:

    # Create PostgreSQL table with optimized settings
    create_table = PostgresOperator(
        task_id='create_postgres_table',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="""
            DROP TABLE IF EXISTS developers;
            CREATE TABLE developers (
                developer TEXT,
                active INTEGER,
                city TEXT,
                autonomous_area TEXT,
                country TEXT,
                established_date TIMESTAMP,
                notable_games TEXT,
                notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) WITH (autovacuum_enabled = true);
            
            -- Create indexes for frequently accessed columns
            CREATE INDEX idx_developer_name ON developers(developer);
            CREATE INDEX idx_established_date ON developers(established_date);
        """
    )

    # Process CSV file
    process_csv_task = PythonOperator(
        task_id='process_csv',
        python_callable=process_csv
    )

    # Load data to PostgreSQL
    load_to_postgres_task = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres
    )

    # Set task dependencies
    create_table >> process_csv_task >> load_to_postgres_task
