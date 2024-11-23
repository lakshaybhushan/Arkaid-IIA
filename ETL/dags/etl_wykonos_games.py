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
        'data_sources/wykonos_games.csv',
        chunksize=chunk_size
    )
    
    for i, chunk in enumerate(chunks):
        # Convert numeric columns
        numeric_columns = ['year', 'metacritic_rating', 'reviewer_rating', 
                         'positivity_ratio', 'to_beat_main', 'to_beat_extra', 
                         'to_beat_completionist', 'extra_content_length']
        for col in numeric_columns:
            chunk[col] = pd.to_numeric(chunk[col], errors='coerce')
            
        chunk.to_csv(f'/tmp/processed_wykonos_games_chunk_{i}.csv', index=False)

def load_to_postgres():
    postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = postgres_hook.get_conn()
    
    with conn.cursor() as cursor:
        cursor.execute("TRUNCATE TABLE wykonos_games;")
        cursor.execute("""
            SET work_mem = '1GB';
            SET maintenance_work_mem = '2GB';
            SET temp_buffers = '1GB';
        """)
    
    chunk_files = [f for f in os.listdir('/tmp') if f.startswith('processed_wykonos_games_chunk_')]
    
    for chunk_file in chunk_files:
        df = pd.read_csv(f'/tmp/{chunk_file}')
        
        buffer = StringIO()
        csv_writer = csv.writer(buffer, 
                              quoting=csv.QUOTE_MINIMAL,
                              escapechar='\\',
                              doublequote=True)
        
        for _, row in df.iterrows():
            csv_writer.writerow([
                str(row['id']),
                str(row['name']),
                float(row['year']),
                float(row.get('metacritic_rating', 0)),
                float(row['reviewer_rating']),
                float(row['positivity_ratio']),
                float(row['to_beat_main']),
                float(row.get('to_beat_extra', 0)),
                float(row['to_beat_completionist']),
                float(row['extra_content_length']),
                str(row['tags'])
            ])
        
        buffer.seek(0)
        
        with conn.cursor() as cursor:
            cursor.execute("""
                SET standard_conforming_strings = on;
                SET escape_string_warning = off;
            """)
            cursor.copy_expert(
                """
                COPY wykonos_games (
                    id,
                    name,
                    year,
                    metacritic_rating,
                    reviewer_rating,
                    positivity_ratio,
                    to_beat_main,
                    to_beat_extra,
                    to_beat_completionist,
                    extra_content_length,
                    tags
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
    'etl_wykonos_games_pipeline',
    default_args=default_args,
    description='ETL pipeline for wykonos games data',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:

    create_table = PostgresOperator(
        task_id='create_postgres_table',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="""
            DROP TABLE IF EXISTS wykonos_games;
            CREATE TABLE wykonos_games (
                id TEXT,
                name TEXT,
                year NUMERIC,
                metacritic_rating NUMERIC,
                reviewer_rating NUMERIC,
                positivity_ratio NUMERIC,
                to_beat_main NUMERIC,
                to_beat_extra NUMERIC,
                to_beat_completionist NUMERIC,
                extra_content_length NUMERIC,
                tags TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) WITH (autovacuum_enabled = true);
            
            CREATE INDEX idx_wykonos_games_id ON wykonos_games(id);
            CREATE INDEX idx_wykonos_games_name ON wykonos_games(name);
            CREATE INDEX idx_wykonos_games_year ON wykonos_games(year);
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