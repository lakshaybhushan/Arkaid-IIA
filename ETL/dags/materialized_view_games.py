import csv
import json
import logging
import hashlib
import tempfile
import pandas as pd
from airflow import DAG
from typing import Dict
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from fuzzywuzzy import fuzz
from fuzzywuzzy import process

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
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

POSTGRES_CONN_ID = 'arkaid_aws_postgres'
SIMILARITY_THRESHOLD = 0.9

def load_mapping():
    """Load the mapping configuration from JSON file"""
    logger.info("Starting to load mapping configuration")
    mapping_path = 'dags/mappings/games_mappings.json'
    
    try:
        with open(mapping_path, 'r') as file:
            mapping = json.load(file)
            
            logger.info("=== Mapping Configuration Details ===")
            logger.info(f"Source Table: {mapping['source_table']}")
            logger.info(f"Destination Table: {mapping['destination_table']}")
            logger.info("\nColumn Mappings:")
            for dest_col, source_col in mapping['column_mappings'].items():
                logger.info(f"  {dest_col} -> {source_col}")
            
            return mapping
    except Exception as e:
        logger.error(f"Error loading mapping file: {str(e)}")
        raise

def get_current_view_definition(postgres_conn_id, view_name):
    """Get the current view definition from the database"""
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    
    sql = """
    SELECT pg_get_viewdef(%s, true) as view_def
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relname = %s
    AND c.relkind = 'm';
    """
    
    try:
        result = hook.get_first(sql, parameters=[view_name, view_name])
        return result[0] if result else None
    except Exception as e:
        logger.error(f"Error getting view definition: {str(e)}")
        return None

def generate_view_hash(sql):
    """Generate a hash of the view definition"""
    return hashlib.md5(sql.encode()).hexdigest()

def process_developer_matches(games_df: pd.DataFrame, developers_df: pd.DataFrame) -> Dict[str, str]:
    """Process developer matches using FuzzyWuzzy"""
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
                # Check game name similarity using FuzzyWuzzy
                for notable_game in dev_info['notable_games']:
                    ratio = fuzz.ratio(game_name, notable_game) / 100.0  # Convert to decimal
                    if ratio >= SIMILARITY_THRESHOLD:
                        dev_mapping[game['id']] = dev_info['id']
                        logger.debug(f"Matched game '{game_name}' with developer '{dev_name}' (similarity: {ratio:.2f})")
                        break
    
    logger.info(f"Completed developer matching. Found {len(dev_mapping)} matches")
    return dev_mapping

def process_publisher_matches(games_df: pd.DataFrame, publishers_df: pd.DataFrame) -> Dict[str, str]:
    """Process publisher matches using FuzzyWuzzy"""
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
                # Check game name similarity using FuzzyWuzzy
                for notable_game in pub_info['notable_games']:
                    ratio = fuzz.ratio(game_name, notable_game) / 100.0  # Convert to decimal
                    if ratio >= SIMILARITY_THRESHOLD:
                        pub_mapping[game['id']] = pub_info['id']
                        logger.debug(f"Matched game '{game_name}' with publisher '{pub_name}' (similarity: {ratio:.2f})")
                        break
    
    logger.info(f"Completed publisher matching. Found {len(pub_mapping)} matches")
    return pub_mapping

def generate_materialized_view_sql():
    """Generate SQL for creating/replacing materialized view"""
    logger.info("Starting to generate materialized view SQL")
    
    mapping = load_mapping()
    source_table = mapping['source_table']
    dest_table = mapping['destination_table']
    column_mappings = mapping['column_mappings']
    
    # Create column mappings for SELECT statement
    select_columns = []
    for dest_col, source_col in column_mappings.items():
        select_columns.append(f"{source_col} as {dest_col}")
    
    columns_sql = ",\n    ".join(select_columns)
    
    sql = f"""
    CREATE MATERIALIZED VIEW IF NOT EXISTS {dest_table} AS
    SELECT 
    {columns_sql}
    FROM {source_table};
    
    -- Create indexes on the materialized view
    CREATE INDEX IF NOT EXISTS idx_mv_games_game_id ON {dest_table}(game_id);
    CREATE INDEX IF NOT EXISTS idx_mv_games_game_name ON {dest_table}(game_name);
    
    -- Analyze the view for performance optimization
    ANALYZE {dest_table};
    """
    
    logger.info("Generated SQL for materialized view")
    return sql

def check_and_recreate_view():
    """Check if view needs to be recreated and generate appropriate SQL"""
    logger.info("Checking if view needs to be recreated")
    
    mapping = load_mapping()
    dest_table = mapping['destination_table']
    
    # Generate new view SQL
    new_view_sql = f"""
        -- Enable pg_trgm extension if not exists
        CREATE EXTENSION IF NOT EXISTS pg_trgm;
        
        {generate_materialized_view_sql()}
    """
    new_view_hash = generate_view_hash(new_view_sql)
    
    # Get current view definition
    current_view_def = get_current_view_definition(POSTGRES_CONN_ID, dest_table)
    
    if current_view_def:
        current_view_hash = generate_view_hash(current_view_def)
        
        if current_view_hash != new_view_hash:
            logger.info("View definition has changed, recreating view")
            return f"""
                -- Enable pg_trgm extension if not exists
                CREATE EXTENSION IF NOT EXISTS pg_trgm;
                
                DROP MATERIALIZED VIEW IF EXISTS {dest_table} CASCADE;
                {new_view_sql}
            """
        else:
            logger.info("View definition unchanged, skipping recreation")
            return None
    else:
        logger.info("View does not exist, creating new view")
        return f"""
            -- Enable pg_trgm extension if not exists
            CREATE EXTENSION IF NOT EXISTS pg_trgm;
            
            {new_view_sql}
        """

def generate_refresh_sql():
    """Generate SQL for refreshing the materialized view"""
    mapping = load_mapping()
    return f"REFRESH MATERIALIZED VIEW {mapping['destination_table']};"

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
                
                # Compare structure of materialized view with expected structure
                cur.execute("""
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_name = 'mv_games'
                    ORDER BY ordinal_position;
                """)
                existing_columns = {col: dtype for col, dtype in cur.fetchall()}
                
                # Load current mapping configuration
                mapping = load_mapping()
                
                dev_changed = existing_dev_mappings != dev_mapping
                pub_changed = existing_pub_mappings != pub_mapping
                
                if dev_changed or pub_changed:
                    logger.info("Changes detected:")
                    if dev_changed:
                        logger.info("- Developer mappings changed")
                    if pub_changed:
                        logger.info("- Publisher mappings changed")
                    return True
                
                logger.info("No changes detected in mappings")
                return False
                
    except Exception as e:
        logger.error(f"Error checking mappings: {str(e)}")
        # If there's an error checking, assume we need to recreate
        return True

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

# Task to create/update materialized view
create_or_update_mv = PostgresOperator(
    task_id='create_or_update_materialized_view',
    postgres_conn_id=POSTGRES_CONN_ID,
    sql=check_and_recreate_view(),
    dag=dag
)

# Task to refresh materialized view
refresh_mv = PostgresOperator(
    task_id='refresh_materialized_view',
    postgres_conn_id=POSTGRES_CONN_ID,
    sql=generate_refresh_sql(),
    dag=dag
)

# Set task dependencies
match_dev_pub >> create_or_update_mv >> refresh_mv

logger.info("DAG setup completed")