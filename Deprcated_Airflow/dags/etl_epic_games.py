from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import pandas as pd
from datetime import datetime, timedelta
from io import StringIO
import csv
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

POSTGRES_CONN_ID = 'arkaid_aws_postgres'

def process_csv():
    chunk_size = 100000
    
    chunks = pd.read_csv(
        'data_sources/epic_games.csv',
        chunksize=chunk_size,
        parse_dates=['release_date'],
        date_parser=lambda x: pd.to_datetime(x, format='%Y-%m-%dT%H:%M:%S.%fZ', errors='coerce')
    )
    
    for i, chunk in enumerate(chunks):
        # Convert price to numeric
        chunk['price'] = pd.to_numeric(chunk['price'], errors='coerce')
        
        # Ensure proper date format
        chunk['release_date'] = pd.to_datetime(chunk['release_date'], format='%Y-%m-%dT%H:%M:%S.%fZ', errors='coerce')
        
        chunk.to_csv(f'/tmp/processed_epic_games_chunk_{i}.csv', index=False)

def load_to_postgres():
    postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = postgres_hook.get_conn()
    
    with conn.cursor() as cursor:
        cursor.execute("TRUNCATE TABLE epic_games;")
        cursor.execute("""
            SET work_mem = '1GB';
            SET maintenance_work_mem = '2GB';
            SET temp_buffers = '1GB';
        """)
    
    chunk_files = [f for f in os.listdir('/tmp') if f.startswith('processed_epic_games_chunk_')]
    
    for chunk_file in chunk_files:
        df = pd.read_csv(f'/tmp/{chunk_file}')
        
        buffer = StringIO()
        csv_writer = csv.writer(buffer, 
                              quoting=csv.QUOTE_MINIMAL,
                              escapechar='\\',
                              doublequote=True)
        
        for _, row in df.iterrows():
            csv_writer.writerow([
                str(row['game_id']),
                str(row['name']),
                str(row['game_slug']),
                float(row['price']),
                row['release_date'],
                str(row['platform']),
                str(row['description']),
                str(row['developer']),
                str(row['publisher']),
                str(row['genres'])
            ])
        
        buffer.seek(0)
        
        with conn.cursor() as cursor:
            cursor.execute("""
                SET standard_conforming_strings = on;
                SET escape_string_warning = off;
            """)
            cursor.copy_expert(
                """
                COPY epic_games (
                    game_id,
                    name,
                    game_slug,
                    price,
                    release_date,
                    platform,
                    description,
                    developer,
                    publisher,
                    genres
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
        
        os.remove(f'/tmp/{chunk_file}')
    
    conn.commit()
    conn.close()

with DAG(
    'etl_epic_games_pipeline',
    default_args=default_args,
    description='ETL pipeline for epic games data',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:

    create_table = PostgresOperator(
        task_id='create_postgres_table',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="""
            DROP TABLE IF EXISTS epic_games;
            CREATE TABLE epic_games (
                game_id TEXT,
                name TEXT,
                game_slug TEXT,
                price NUMERIC,
                release_date TIMESTAMP,
                platform TEXT,
                description TEXT,
                developer TEXT,
                publisher TEXT,
                genres TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) WITH (autovacuum_enabled = true);
            
            CREATE INDEX idx_epic_games_id ON epic_games(game_id);
            CREATE INDEX idx_epic_games_name ON epic_games(name);
            CREATE INDEX idx_epic_games_release_date ON epic_games(release_date);
            CREATE INDEX idx_epic_games_publisher ON epic_games(publisher);
            CREATE INDEX idx_epic_games_developer ON epic_games(developer);
        """
    )

    process_csv_task = PythonOperator(
        task_id='process_csv',
        python_callable=process_csv
    )

    load_to_postgres_task = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres
    )

    create_table >> process_csv_task >> load_to_postgres_task 