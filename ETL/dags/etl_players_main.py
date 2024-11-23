from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.log.logging_mixin import LoggingMixin

import pandas as pd
from datetime import datetime, timedelta
from io import StringIO
import csv
import os
import gc
import logging
import ast

# Set up logger
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

POSTGRES_CONN_ID = 'arkaid_aws_postgres'

def clean_array_field(field_str):
    if pd.isna(field_str):
        return '[]'
    try:
        # Convert string representation of list to actual list
        array_list = ast.literal_eval(field_str)
        return str(array_list)
    except:
        return '[]'

def process_csv():
    logger.info("Starting CSV processing")
    chunk_size = 50000  # Large chunk size for better performance
    
    try:
        chunks = pd.read_csv(
            'data_sources/players_main.csv',
            chunksize=chunk_size,
            na_values=['', 'null', 'NULL', '[]'],
            parse_dates=['DOB']
        )
        
        total_rows = 0
        for i, chunk in enumerate(chunks):
            logger.info(f"Processing chunk {i+1} with {len(chunk)} rows")
            
            # Convert DOB to proper date format
            chunk['DOB'] = pd.to_datetime(chunk['DOB']).dt.strftime('%Y-%m-%d')
            
            # Clean array fields
            array_columns = ['Library_All', 'Library_Favorites', 'Library_Playing', 
                           'Library_Title', 'Wishlist', 'PaymentMethod']
            for col in array_columns:
                chunk[col] = chunk[col].apply(clean_array_field)
            
            # Convert Age to numeric
            chunk['Age'] = pd.to_numeric(chunk['Age'], errors='coerce')
            
            # Save chunk and clear memory
            chunk.to_csv(f'/tmp/processed_players_chunk_{i}.csv', index=False)
            total_rows += len(chunk)
            logger.info(f"Saved chunk {i+1} to temporary file. Total rows processed: {total_rows}")
            
            del chunk
            gc.collect()

        logger.info(f"Completed CSV processing. Total chunks: {i+1}, Total rows: {total_rows}")
        
    except Exception as e:
        logger.error(f"Error in process_csv: {str(e)}")
        raise

def load_to_postgres():
    logger.info("Starting PostgreSQL load")
    postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = postgres_hook.get_conn()
    
    try:
        with conn.cursor() as cursor:
            logger.info("Truncating existing table")
            cursor.execute("TRUNCATE TABLE players_main;")
            cursor.execute("""
                SET work_mem = '256MB';           -- Increased for better performance
                SET maintenance_work_mem = '512MB';
                SET temp_buffers = '128MB';
            """)
        
        chunk_files = [f for f in os.listdir('/tmp') if f.startswith('processed_players_chunk_')]
        total_chunks = len(chunk_files)
        logger.info(f"Found {total_chunks} chunks to process")
        
        rows_loaded = 0
        for chunk_num, chunk_file in enumerate(chunk_files, 1):
            logger.info(f"Processing chunk file {chunk_num}/{total_chunks}: {chunk_file}")
            
            df = pd.read_csv(f'/tmp/{chunk_file}', chunksize=10000)
            
            mini_chunk_count = 0
            for mini_chunk in df:
                mini_chunk_count += 1
                buffer = StringIO()
                csv_writer = csv.writer(buffer, 
                                      quoting=csv.QUOTE_MINIMAL,
                                      escapechar='\\',
                                      doublequote=True)
                
                for _, row in mini_chunk.iterrows():
                    csv_writer.writerow([
                        str(row['ID']),
                        str(row['Name']),
                        str(row['Username']),
                        int(row['Age']),
                        row['DOB'],
                        str(row['RecentlyPlayed']),
                        str(row['Email']),
                        str(row['Phone']),
                        str(row['Library_All']),
                        str(row['Library_Favorites']),
                        str(row['Library_Playing']),
                        str(row['Library_Title']),
                        str(row['Wishlist']),
                        str(row['PaymentMethod']),
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
                        COPY players_main (
                            id,
                            name,
                            username,
                            age,
                            dob,
                            recently_played,
                            email,
                            phone,
                            library_all,
                            library_favorites,
                            library_playing,
                            library_title,
                            wishlist,
                            payment_method,
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
                
                rows_loaded += len(mini_chunk)
                logger.info(f"Loaded mini-chunk {mini_chunk_count} of chunk {chunk_num}. Total rows loaded: {rows_loaded}")
                
                del mini_chunk
                gc.collect()
            
            os.remove(f'/tmp/{chunk_file}')
            logger.info(f"Completed chunk {chunk_num}/{total_chunks}. Removed temporary file.")
        
        conn.commit()
        logger.info(f"Successfully completed loading {rows_loaded} rows to PostgreSQL")
        
    except Exception as e:
        logger.error(f"Error in load_to_postgres: {str(e)}")
        raise
    finally:
        conn.close()
        logger.info("Closed PostgreSQL connection")

with DAG(
    'etl_players_main_pipeline',
    default_args=default_args,
    description='ETL pipeline for players main data',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:

    create_table = PostgresOperator(
        task_id='create_postgres_table',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="""
            DROP TABLE IF EXISTS players_main;
            CREATE TABLE players_main (
                id TEXT,
                name TEXT,
                username TEXT,
                age INTEGER,
                dob DATE,
                recently_played TEXT,
                email TEXT,
                phone TEXT,
                library_all TEXT,
                library_favorites TEXT,
                library_playing TEXT,
                library_title TEXT,
                wishlist TEXT,
                payment_method TEXT,
                country TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) WITH (autovacuum_enabled = true);
            
            CREATE INDEX idx_players_id ON players_main(id);
            CREATE INDEX idx_players_username ON players_main(username);
            CREATE INDEX idx_players_email ON players_main(email);
            CREATE INDEX idx_players_dob ON players_main(dob);
            CREATE INDEX idx_players_country ON players_main(country);
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