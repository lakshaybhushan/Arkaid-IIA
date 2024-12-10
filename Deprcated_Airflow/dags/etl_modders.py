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
        'data_sources/modders.csv',
        chunksize=chunk_size
    )
    
    for i, chunk in enumerate(chunks):
        chunk.to_csv(f'/tmp/processed_modders_chunk_{i}.csv', index=False)

def load_to_postgres():
    postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = postgres_hook.get_conn()
    
    with conn.cursor() as cursor:
        cursor.execute("TRUNCATE TABLE modders;")
        
        cursor.execute("""
            SET work_mem = '1GB';
            SET maintenance_work_mem = '2GB';
            SET temp_buffers = '1GB';
        """)
    
    chunk_files = [f for f in os.listdir('/tmp') if f.startswith('processed_modders_chunk_')]
    
    for chunk_file in chunk_files:
        df = pd.read_csv(f'/tmp/{chunk_file}')
        
        buffer = StringIO()
        csv_writer = csv.writer(buffer, 
                              quoting=csv.QUOTE_MINIMAL,
                              escapechar='\\',
                              doublequote=True)
        
        for _, row in df.iterrows():
            csv_writer.writerow([
                str(row['ID']),
                str(row['Name']),
                str(row['Primary_Game_ID']),
                str(row['Type_of_Mod']),
                str(row['Country'])
            ])
        
        buffer.seek(0)
        
        with conn.cursor() as cursor:
            cursor.execute("""
                SET standard_conforming_strings = on;
                SET escape_string_warning = off;
            """)
            cursor.copy_expert(
                """
                COPY modders (
                    id,
                    name,
                    primary_game_id,
                    type_of_mod,
                    country
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
    'etl_modders_pipeline',
    default_args=default_args,
    description='ETL pipeline for modders data',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:

    create_table = PostgresOperator(
        task_id='create_postgres_table',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="""
            DROP TABLE IF EXISTS modders;
            CREATE TABLE modders (
                id TEXT,
                name TEXT,
                primary_game_id TEXT,
                type_of_mod TEXT,
                country TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) WITH (autovacuum_enabled = true);
            
            CREATE INDEX idx_modder_id ON modders(id);
            CREATE INDEX idx_modder_game_id ON modders(primary_game_id);
            CREATE INDEX idx_modder_country ON modders(country);
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