from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json
import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from typing import Dict
import tempfile
import csv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,  # Increased retries for robustness
    'retry_delay': timedelta(minutes=5),
}

POSTGRES_CONN_ID = 'arkaid_aws_postgres'
SIMILARITY_THRESHOLD = 0.9  # Jaccard similarity threshold

def levenshtein_distance(s1: str, s2: str) -> int:
    """Calculate the Levenshtein distance between two strings"""
    if len(s1) < len(s2):
        return levenshtein_distance(s2, s1)

    if len(s2) == 0:
        return len(s1)

    previous_row = range(len(s2) + 1)
    for i, c1 in enumerate(s1):
        current_row = [i + 1]
        for j, c2 in enumerate(s2):
            insertions = previous_row[j + 1] + 1
            deletions = current_row[j] + 1
            substitutions = previous_row[j] + (c1 != c2)
            current_row.append(min(insertions, deletions, substitutions))
        previous_row = current_row

    return previous_row[-1]

def process_developer_matches(games_df: pd.DataFrame, developers_df: pd.DataFrame) -> Dict[str, str]:
    """Process developer matches using Levenshtein distance"""
    logger.info("Starting developer matching process")
    
    # Prepare data
    games_df['name_lower'] = games_df['name'].str.lower().str.strip()
    developers_df['developer_name_lower'] = developers_df['developer_name'].str.lower().str.strip()
    
    # Create dictionary for faster lookups
    dev_dict = {}
    for _, dev in developers_df.iterrows():
        if pd.notna(dev['notable_games']):
            notable_games = [g.lower().strip() for g in str(dev['notable_games']).split(',')]
            dev_dict[dev['developer_name_lower']] = {
                'id': dev['developer_id'],
                'notable_games': notable_games
            }
    
    logger.info(f"Processed {len(dev_dict)} developers")
    
    dev_mapping = {}
    total_games = len(games_df)
    
    for idx, game in games_df.iterrows():
        if idx % 1000 == 0:
            logger.info(f"Processing game {idx + 1}/{total_games}")
        
        if pd.isna(game['developers']):
            continue
        
        game_devs = [d.lower().strip() for d in str(game['developers']).split(',')]
        game_name = game['name_lower']
        
        for dev_name in game_devs:
            if dev_name in dev_dict:
                dev_info = dev_dict[dev_name]
                # Check game name similarity using Levenshtein distance
                for notable_game in dev_info['notable_games']:
                    distance = levenshtein_distance(game_name, notable_game)
                    if distance <= 3:  # Maximum allowed distance
                        dev_mapping[game['id']] = dev_info['id']
                        logger.debug(f"Matched game '{game_name}' with developer '{dev_name}' (distance: {distance})")
                        break
    
    logger.info(f"Completed developer matching. Found {len(dev_mapping)} matches")
    return dev_mapping

def process_publisher_matches(games_df: pd.DataFrame, publishers_df: pd.DataFrame) -> Dict[str, str]:
    """Process publisher matches using Levenshtein distance"""
    logger.info("Starting publisher matching process")
    
    # Prepare data
    games_df['name_lower'] = games_df['name'].str.lower().str.strip()
    publishers_df['publisher_name_lower'] = publishers_df['publisher_name'].str.lower().str.strip()
    
    # Create dictionary for faster lookups
    pub_dict = {}
    for _, pub in publishers_df.iterrows():
        if pd.notna(pub['notable_games_published']):
            notable_games = [g.lower().strip() for g in str(pub['notable_games_published']).split(',')]
            pub_dict[pub['publisher_name_lower']] = {
                'id': pub['publisher_id'],
                'notable_games': notable_games
            }
    
    logger.info(f"Processed {len(pub_dict)} publishers")
    
    pub_mapping = {}
    total_games = len(games_df)
    
    for idx, game in games_df.iterrows():
        if idx % 1000 == 0:
            logger.info(f"Processing game {idx + 1}/{total_games}")
        
        if pd.isna(game['publishers']):
            continue
        
        game_pubs = [p.lower().strip() for p in str(game['publishers']).split(',')]
        game_name = game['name_lower']
        
        for pub_name in game_pubs:
            if pub_name in pub_dict:
                pub_info = pub_dict[pub_name]
                # Check game name similarity using Levenshtein distance
                for notable_game in pub_info['notable_games']:
                    distance = levenshtein_distance(game_name, notable_game)
                    if distance <= 3:  # Maximum allowed distance
                        pub_mapping[game['id']] = pub_info['id']
                        logger.debug(f"Matched game '{game_name}' with publisher '{pub_name}' (distance: {distance})")
                        break
    
    logger.info(f"Completed publisher matching. Found {len(pub_mapping)} matches")
    return pub_mapping

def create_mapping_tables(hook, dev_mapping: Dict[str, str], pub_mapping: Dict[str, str], batch_size: int = 10000):
    """Create mapping tables and insert data in optimized batches"""
    logger.info("Starting mapping table creation with batch processing")
    
    try:
        # Establish connection with autocommit to handle DDL statements properly
        conn = hook.get_conn()
        conn.set_session(autocommit=True)
        
        with conn.cursor() as cur:
            # Drop existing mapping tables if they exist
            logger.info("Dropping existing mapping tables if they exist")
            cur.execute("""
                DROP TABLE IF EXISTS dev_mapping;
                DROP TABLE IF EXISTS pub_mapping;
            """)
            logger.info("Creating new mapping tables")
            cur.execute("""
                CREATE TABLE dev_mapping (
                    game_id TEXT,
                    developer_id TEXT
                );
                
                CREATE TABLE pub_mapping (
                    game_id TEXT,
                    publisher_id TEXT
                );
            """)
            
            # Function to bulk insert using COPY from temporary CSV
            def bulk_insert(mapping, table_name):
                if not mapping:
                    logger.warning(f"No data to insert for {table_name}")
                    return
                
                items = list(mapping.items())
                total_items = len(items)
                logger.info(f"Processing {total_items} records for {table_name}")
                
                with tempfile.NamedTemporaryFile(mode='w+', newline='', delete=True) as temp_file:
                    writer = csv.writer(temp_file)
                    writer.writerows(items)
                    temp_file.flush()
                    temp_file.seek(0)
                    
                    logger.info(f"Bulk inserting into {table_name}")
                    cur.copy_from(
                        file=temp_file,
                        table=table_name,
                        columns=('game_id', 'developer_id') if table_name == 'dev_mapping' else ('game_id', 'publisher_id'),
                        sep=','
                    )
                    logger.info(f"Completed bulk insert for {table_name}")
            
            # Bulk insert developer mappings
            bulk_insert(dev_mapping, 'dev_mapping')
            
            # Bulk insert publisher mappings
            bulk_insert(pub_mapping, 'pub_mapping')
            
            # Create indexes after data insertion for better performance
            logger.info("Creating indexes on mapping tables")
            cur.execute("""
                CREATE INDEX idx_dev_game_id ON dev_mapping(game_id);
                CREATE INDEX idx_pub_game_id ON pub_mapping(game_id);
            """)
            
            # Analyze tables to optimize query planning
            logger.info("Analyzing mapping tables")
            cur.execute("""
                ANALYZE dev_mapping;
                ANALYZE pub_mapping;
            """)
            
            # Log statistics
            cur.execute("SELECT COUNT(*) FROM dev_mapping")
            dev_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM pub_mapping")
            pub_count = cur.fetchone()[0]
            
            logger.info(f"Final statistics: {dev_count} developer mappings, {pub_count} publisher mappings")
        
        logger.info("Successfully completed mapping table creation")
        
    except Exception as e:
        logger.error(f"Error in create_mapping_tables: {str(e)}")
        raise
    finally:
        if conn:
            conn.close()

def check_mappings_changed(hook, dev_mapping: Dict[str, str], pub_mapping: Dict[str, str]) -> bool:
    """Check if the mappings have changed compared to existing tables"""
    logger.info("Checking if mappings have changed")
    
    try:
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                # Check if mapping tables exist
                cur.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = 'dev_mapping'
                    ) AND EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = 'pub_mapping'
                    );
                """)
                tables_exist = cur.fetchone()[0]
                
                if not tables_exist:
                    logger.info("Mapping tables don't exist, need to create them")
                    return True
                
                # Compare existing mappings with new ones
                cur.execute("SELECT game_id, developer_id FROM dev_mapping")
                existing_dev_mappings = dict(cur.fetchall())
                
                cur.execute("SELECT game_id, publisher_id FROM pub_mapping")
                existing_pub_mappings = dict(cur.fetchall())
                
                dev_changed = existing_dev_mappings != dev_mapping
                pub_changed = existing_pub_mappings != pub_mapping
                
                if dev_changed or pub_changed:
                    logger.info("Mappings have changed, need to recreate tables")
                    return True
                
                logger.info("Mappings haven't changed, no need to recreate tables")
                return False
                
    except Exception as e:
        logger.error(f"Error checking mappings: {str(e)}")
        # If there's an error checking, assume we need to recreate
        return True

def match_developer_publisher():
    """Main function to match developers and publishers"""
    logger.info("Starting developer and publisher matching process")
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    
    try:
        # Load and process data as before
        games_df = pd.read_sql("""
            SELECT id, name, developers, publishers 
            FROM (
                SELECT id, name, developers, publishers 
                FROM steam_games 
                UNION ALL 
                SELECT game_id as id, name, developer as developers, publisher as publishers 
                FROM epic_games
            ) combined_games
        """, hook.get_conn())
        logger.info(f"Loaded {len(games_df)} games records")
        
        developers_df = pd.read_sql("""
            SELECT developer_id, developer_name, notable_games 
            FROM mv_developers 
            WHERE notable_games IS NOT NULL
        """, hook.get_conn())
        logger.info(f"Loaded {len(developers_df)} developers records")
        
        publishers_df = pd.read_sql("""
            SELECT publisher_id, publisher_name, notable_games_published 
            FROM mv_publishers 
            WHERE notable_games_published IS NOT NULL
        """, hook.get_conn())
        logger.info(f"Loaded {len(publishers_df)} publishers records")
        
        # Process matches
        dev_mapping = process_developer_matches(games_df, developers_df)
        pub_mapping = process_publisher_matches(games_df, publishers_df)
        
        # Check if mappings have changed
        if check_mappings_changed(hook, dev_mapping, pub_mapping):
            logger.info("Creating new mapping tables")
            # Drop materialized view first to remove dependencies
            with hook.get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("DROP MATERIALIZED VIEW IF EXISTS mv_games CASCADE;")
            # Create new mapping tables
            create_mapping_tables(hook, dev_mapping, pub_mapping)
        else:
            logger.info("Mappings unchanged, skipping recreation")
        
        logger.info("Successfully completed matching process")
        
    except Exception as e:
        logger.error(f"Error in match_developer_publisher: {str(e)}")
        raise

def generate_materialized_view_sql():
    """Generate SQL for creating/replacing materialized view"""
    logger.info("Generating materialized view SQL")
    sql = """
    -- Drop existing view if it exists
    DROP MATERIALIZED VIEW IF EXISTS mv_games CASCADE;
    
    -- Create the new materialized view
    CREATE MATERIALIZED VIEW mv_games AS
    SELECT 
        cg.id as game_id,
        cg.name,
        TO_CHAR(cg.release_date::date, 'YYYY-MM-DD') as release_date,
        cg.description,
        cg.price,
        cg.genres,
        dm.developer_id,
        pm.publisher_id,
        array_to_string(ARRAY[
            CASE WHEN cg.windows = 1 THEN 'windows' END,
            CASE WHEN cg.mac = 1 THEN 'mac' END,
            CASE WHEN cg.linux = 1 THEN 'linux' END
        ], ',') as platforms,
        CASE 
            WHEN cg.id IN (SELECT id FROM steam_games) THEN cg.metacritic_score
            ELSE NULL
        END as ratings
    FROM (
        SELECT id, name, release_date::date, about_game as description, price, genres, 
               CAST(windows AS INTEGER) as windows, CAST(mac AS INTEGER) as mac, 
               CAST(linux AS INTEGER) as linux, metacritic_score
        FROM steam_games
        UNION ALL
        SELECT game_id as id, name, release_date::date, description, price, genres,
               1 as windows, 0 as mac, 0 as linux, 
               NULL as metacritic_score
        FROM epic_games
    ) cg
    LEFT JOIN dev_mapping dm ON cg.id = dm.game_id
    LEFT JOIN pub_mapping pm ON cg.id = pm.game_id
    WITH DATA;
    
    -- Create indexes on the materialized view
    CREATE INDEX idx_mv_games_game_id ON mv_games(game_id);
    CREATE INDEX idx_mv_games_name ON mv_games(name);
    
    -- Analyze the view for performance optimization
    ANALYZE mv_games;
    """
    logger.info("Generated materialized view SQL")
    return sql

# Create the DAG
dag = DAG(
    'materialized_view_games',
    default_args=default_args,
    description='Creates and refreshes materialized view for games data',
    schedule_interval='@daily',
    catchup=False
)

# Task to match developers and publishers
match_dev_pub = PythonOperator(
    task_id='match_developers_publishers',
    python_callable=match_developer_publisher,
    dag=dag
)

# Task to create materialized view
create_mv = PostgresOperator(
    task_id='create_materialized_view',
    postgres_conn_id=POSTGRES_CONN_ID,
    sql=generate_materialized_view_sql(),
    dag=dag
)

# Task to verify the existence of the materialized view
verify_mv = PostgresOperator(
    task_id='verify_materialized_view',
    postgres_conn_id=POSTGRES_CONN_ID,
    sql="""
    DO $$
    DECLARE
        row_count INTEGER;
        view_size TEXT;
    BEGIN
        IF EXISTS (
            SELECT 1
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relname = 'mv_games'
            AND c.relkind = 'm'
        ) THEN
            SELECT COUNT(*) INTO row_count FROM mv_games;
            RAISE NOTICE 'Materialized view exists with % rows', row_count;
            
            -- Get view size and schema info
            SELECT pg_size_pretty(pg_total_relation_size('mv_games')) INTO view_size;
            RAISE NOTICE 'View size: %', view_size;
            
            -- Additional verification
            SELECT COUNT(*) INTO row_count 
            FROM mv_games 
            WHERE game_id IS NOT NULL;
            RAISE NOTICE 'Number of games with valid game_id: %', row_count;
        ELSE
            RAISE EXCEPTION 'Materialized view mv_games does not exist';
        END IF;
    END $$;
    """,
    dag=dag
)

# Set task dependencies
match_dev_pub >> create_mv >> verify_mv

logger.info("DAG setup completed")