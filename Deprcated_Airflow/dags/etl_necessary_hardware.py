from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import pandas as pd
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

def process_csv():
    chunk_size = 100000
    
    chunks = pd.read_csv(
        'data_sources/necessary_hardware.csv',
        chunksize=chunk_size
    )
    
    for i, chunk in enumerate(chunks):
        chunk.to_csv(f'/tmp/processed_hardware_chunk_{i}.csv', index=False)

def load_to_postgres():
    postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = postgres_hook.get_conn()
    
    with conn.cursor() as cursor:
        cursor.execute("TRUNCATE TABLE necessary_hardware;")
        
        cursor.execute("""
            SET work_mem = '1GB';
            SET maintenance_work_mem = '2GB';
            SET temp_buffers = '1GB';
        """)
    
    chunk_files = [f for f in os.listdir('/tmp') if f.startswith('processed_hardware_chunk_')]
    
    for chunk_file in chunk_files:
        df = pd.read_csv(f'/tmp/{chunk_file}')
        
        buffer = StringIO()
        csv_writer = csv.writer(buffer, 
                              quoting=csv.QUOTE_MINIMAL,
                              escapechar='\\',
                              doublequote=True)
        
        for _, row in df.iterrows():
            csv_writer.writerow([
                str(row['hardware_id']),
                str(row['operacional_system']),
                str(row['processor']),
                str(row['memory']),
                str(row['graphics']),
                str(row['storage']),
                str(row['fk_game_id'])
            ])
        
        buffer.seek(0)
        
        with conn.cursor() as cursor:
            cursor.execute("""
                SET standard_conforming_strings = on;
                SET escape_string_warning = off;
            """)
            cursor.copy_expert(
                """
                COPY necessary_hardware (
                    hardware_id,
                    operacional_system,
                    processor,
                    memory,
                    graphics,
                    storage,
                    fk_game_id
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
    'etl_necessary_hardware_pipeline',
    default_args=default_args,
    description='ETL pipeline for necessary hardware data',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:

    create_table = PostgresOperator(
        task_id='create_postgres_table',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="""
            DROP TABLE IF EXISTS necessary_hardware;
            CREATE TABLE necessary_hardware (
                hardware_id TEXT,
                operacional_system TEXT,
                processor TEXT,
                memory TEXT,
                graphics TEXT,
                storage TEXT,
                fk_game_id TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) WITH (autovacuum_enabled = true);
            
            CREATE INDEX idx_hardware_id ON necessary_hardware(hardware_id);
            CREATE INDEX idx_hardware_game_id ON necessary_hardware(fk_game_id);
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