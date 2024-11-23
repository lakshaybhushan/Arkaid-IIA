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
import ast
import gc
import logging

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

def clean_date(date_str):
    if pd.isna(date_str) or date_str == 'nan':
        return None
    try:
        return pd.to_datetime(date_str, format='%b %d, %Y').strftime('%Y-%m-%d')
    except:
        return None

def process_csv():
    logger.info("Starting CSV processing")
    chunk_size = 50000  # Increased chunk size
    
    try:
        chunks = pd.read_csv(
            'data_sources/steam_games.csv',
            chunksize=chunk_size,
            na_values=['', 'null', 'NULL', '[]']
        )
        
        total_rows = 0
        for i, chunk in enumerate(chunks):
            logger.info(f"Processing chunk {i+1} with {len(chunk)} rows")
            
            # Convert release date
            chunk['Release date'] = chunk['Release date'].apply(clean_date)
            
            # Convert numeric fields
            numeric_columns = ['Price', 'DLC count', 'Required age', 'Metacritic score',
                             'User score', 'Positive', 'Negative', 'Achievements',
                             'Average playtime forever', 'Average playtime two weeks',
                             'Median playtime forever', 'Median playtime two weeks',
                             'Windows', 'Mac', 'Linux']
            
            for col in numeric_columns:
                chunk[col] = pd.to_numeric(chunk[col], errors='coerce')
            
            # Save chunk and clear memory
            chunk.to_csv(f'/tmp/processed_steam_games_chunk_{i}.csv', index=False)
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
            cursor.execute("TRUNCATE TABLE steam_games;")
            cursor.execute("""
                SET work_mem = '128MB';           -- Increased memory settings
                SET maintenance_work_mem = '256MB';
                SET temp_buffers = '64MB';
            """)
        
        chunk_files = [f for f in os.listdir('/tmp') if f.startswith('processed_steam_games_chunk_')]
        total_chunks = len(chunk_files)
        logger.info(f"Found {total_chunks} chunks to process")
        
        rows_loaded = 0
        for chunk_num, chunk_file in enumerate(chunk_files, 1):
            logger.info(f"Processing chunk file {chunk_num}/{total_chunks}: {chunk_file}")
            
            # Read chunk in larger batches
            df = pd.read_csv(f'/tmp/{chunk_file}', chunksize=10000)  # Increased mini-chunk size
            
            mini_chunk_count = 0
            for mini_chunk in df:
                mini_chunk_count += 1
                buffer = StringIO()
                csv_writer = csv.writer(buffer, 
                                      quoting=csv.QUOTE_MINIMAL,
                                      escapechar='\\',
                                      doublequote=True)
                
                for _, row in mini_chunk.iterrows():
                    release_date = row['Release date']
                    if pd.isna(release_date) or release_date == 'nan':
                        release_date = ''
                    
                    csv_writer.writerow([
                        str(row['id']),
                        str(row['Name']),
                        release_date,
                        str(row['Estimated owners']),
                        float(row.get('Peak CCU', 0)),
                        int(row.get('Required age', 0)),
                        float(row.get('Price', 0)),
                        int(row.get('DLC count', 0)),
                        str(row['About the game']),
                        str(row['Supported languages']),
                        str(row['Full audio languages']),
                        str(row.get('Reviews', '')),
                        str(row['Header image']),
                        str(row.get('Website', '')),
                        str(row.get('Support url', '')),
                        str(row.get('Support email', '')),
                        int(row.get('Windows', 0)),
                        int(row.get('Mac', 0)),
                        int(row.get('Linux', 0)),
                        float(row.get('Metacritic score', 0)),
                        str(row.get('Metacritic url', '')),
                        float(row.get('User score', 0)),
                        int(row.get('Positive', 0)),
                        int(row.get('Negative', 0)),
                        str(row.get('Score rank', '')),
                        int(row.get('Achievements', 0)),
                        int(row.get('Recommendations', 0)),
                        str(row.get('Notes', '')),
                        float(row.get('Average playtime forever', 0)),
                        float(row.get('Average playtime two weeks', 0)),
                        float(row.get('Median playtime forever', 0)),
                        float(row.get('Median playtime two weeks', 0)),
                        str(row['Developers']),
                        str(row['Publishers']),
                        str(row['Categories']),
                        str(row['Genres']),
                        str(row['Tags']),
                        str(row['Screenshots']),
                        str(row['Movies'])
                    ])
                
                buffer.seek(0)
                
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SET standard_conforming_strings = on;
                        SET escape_string_warning = off;
                    """)
                    cursor.copy_expert(
                        """
                        COPY steam_games (
                            id, name, release_date, estimated_owners, peak_ccu, required_age,
                            price, dlc_count, about_game, supported_languages, full_audio_languages,
                            reviews, header_image, website, support_url, support_email,
                            windows, mac, linux, metacritic_score, metacritic_url,
                            user_score, positive, negative, score_rank, achievements,
                            recommendations, notes, avg_playtime_forever, avg_playtime_two_weeks,
                            median_playtime_forever, median_playtime_two_weeks,
                            developers, publishers, categories, genres, tags,
                            screenshots, movies
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
    'etl_steam_games_pipeline',
    default_args=default_args,
    description='ETL pipeline for steam games data',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:

    create_table = PostgresOperator(
        task_id='create_postgres_table',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="""
            DROP TABLE IF EXISTS steam_games;
            CREATE TABLE steam_games (
                id TEXT,
                name TEXT,
                release_date DATE,
                estimated_owners TEXT,
                peak_ccu NUMERIC,
                required_age INTEGER,
                price NUMERIC,
                dlc_count INTEGER,
                about_game TEXT,
                supported_languages TEXT,
                full_audio_languages TEXT,
                reviews TEXT,
                header_image TEXT,
                website TEXT,
                support_url TEXT,
                support_email TEXT,
                windows INTEGER,
                mac INTEGER,
                linux INTEGER,
                metacritic_score NUMERIC,
                metacritic_url TEXT,
                user_score NUMERIC,
                positive INTEGER,
                negative INTEGER,
                score_rank TEXT,
                achievements INTEGER,
                recommendations INTEGER,
                notes TEXT,
                avg_playtime_forever NUMERIC,
                avg_playtime_two_weeks NUMERIC,
                median_playtime_forever NUMERIC,
                median_playtime_two_weeks NUMERIC,
                developers TEXT,
                publishers TEXT,
                categories TEXT,
                genres TEXT,
                tags TEXT,
                screenshots TEXT,
                movies TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) WITH (autovacuum_enabled = true);
            
            CREATE INDEX idx_steam_games_id ON steam_games(id);
            CREATE INDEX idx_steam_games_name ON steam_games(name);
            CREATE INDEX idx_steam_games_release_date ON steam_games(release_date);
            CREATE INDEX idx_steam_games_price ON steam_games(price);
            CREATE INDEX idx_steam_games_developers ON steam_games(developers);
            CREATE INDEX idx_steam_games_publishers ON steam_games(publishers);
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