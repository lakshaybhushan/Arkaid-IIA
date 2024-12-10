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
        'data_sources/game_stats.csv',
        chunksize=chunk_size
    )
    
    for i, chunk in enumerate(chunks):
        # Convert numeric columns
        numeric_columns = ['Achievements', 'hrsPlayed', 'avg_critic_rating', 
                         'avg_user_rating', 'total_users', 'total_critics', 
                         'completion_rate', 'daily_active_users', 
                         'monthly_active_users', 'favorites']
        for col in numeric_columns:
            chunk[col] = pd.to_numeric(chunk[col], errors='coerce')
            
        chunk.to_csv(f'/tmp/processed_game_stats_chunk_{i}.csv', index=False)

def load_to_postgres():
    postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = postgres_hook.get_conn()
    
    with conn.cursor() as cursor:
        cursor.execute("TRUNCATE TABLE game_stats;")
        cursor.execute("""
            SET work_mem = '1GB';
            SET maintenance_work_mem = '2GB';
            SET temp_buffers = '1GB';
        """)
    
    chunk_files = [f for f in os.listdir('/tmp') if f.startswith('processed_game_stats_chunk_')]
    
    for chunk_file in chunk_files:
        df = pd.read_csv(f'/tmp/{chunk_file}')
        
        buffer = StringIO()
        csv_writer = csv.writer(buffer, 
                              quoting=csv.QUOTE_MINIMAL,
                              escapechar='\\',
                              doublequote=True)
        
        for _, row in df.iterrows():
            csv_writer.writerow([
                str(row['gameID']),
                str(row['Name']),
                str(row['Genres']),
                int(row['Achievements']),
                str(row['Difficulty Level']),
                float(row['hrsPlayed']),
                float(row['avg_critic_rating']),
                float(row['avg_user_rating']),
                int(row['total_users']),
                int(row['total_critics']),
                str(row['age_rating']),
                float(row['completion_rate']),
                int(row['daily_active_users']),
                int(row['monthly_active_users']),
                int(row['favorites'])
            ])
        
        buffer.seek(0)
        
        with conn.cursor() as cursor:
            cursor.execute("""
                SET standard_conforming_strings = on;
                SET escape_string_warning = off;
            """)
            cursor.copy_expert(
                """
                COPY game_stats (
                    game_id,
                    name,
                    genres,
                    achievements,
                    difficulty_level,
                    hrs_played,
                    avg_critic_rating,
                    avg_user_rating,
                    total_users,
                    total_critics,
                    age_rating,
                    completion_rate,
                    daily_active_users,
                    monthly_active_users,
                    favorites
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
    'etl_game_stats_pipeline',
    default_args=default_args,
    description='ETL pipeline for game stats data',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:

    create_table = PostgresOperator(
        task_id='create_postgres_table',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="""
            DROP TABLE IF EXISTS game_stats;
            CREATE TABLE game_stats (
                game_id TEXT,
                name TEXT,
                genres TEXT,
                achievements INTEGER,
                difficulty_level TEXT,
                hrs_played NUMERIC,
                avg_critic_rating NUMERIC,
                avg_user_rating NUMERIC,
                total_users INTEGER,
                total_critics INTEGER,
                age_rating TEXT,
                completion_rate NUMERIC,
                daily_active_users INTEGER,
                monthly_active_users INTEGER,
                favorites INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) WITH (autovacuum_enabled = true);
            
            CREATE INDEX idx_game_stats_id ON game_stats(game_id);
            CREATE INDEX idx_game_stats_name ON game_stats(name);
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