from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json
import logging
import hashlib
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

POSTGRES_CONN_ID = 'arkaid_aws_postgres'

def load_mapping():
    """Load the mapping configuration from JSON file"""
    logger.info("Starting to load mapping configuration")
    mapping_path = 'dags/mappings/games_stats_mappings.json'
    
    try:
        with open(mapping_path, 'r') as file:
            mapping = json.load(file)
            
            # Detailed logging of mapping configuration
            logger.info("=== Mapping Configuration Details ===")
            logger.info(f"Source Table: {mapping['source_table']}")
            logger.info(f"Source Type: {mapping['source_type']}")
            logger.info(f"Destination Table: {mapping['destination_table']}")
            logger.info("\nColumn Mappings:")
            for dest_col, source_col in mapping['column_mappings'].items():
                logger.info(f"  {dest_col} -> {source_col}")
            logger.info("================================")
            
            return mapping
    except FileNotFoundError:
        logger.error(f"Mapping file not found at path: {mapping_path}")
        raise
    except json.JSONDecodeError:
        logger.error("Failed to parse mapping JSON file")
        raise
    except Exception as e:
        logger.error(f"Unexpected error loading mapping file: {str(e)}")
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

def check_and_recreate_view():
    """Check if view needs to be recreated and generate appropriate SQL"""
    logger.info("Checking if view needs to be recreated")
    
    mapping = load_mapping()
    dest_table = mapping['destination_table']
    
    # Generate new view SQL
    new_view_sql = generate_materialized_view_sql()
    new_view_hash = generate_view_hash(new_view_sql)
    
    # Get current view definition
    current_view_def = get_current_view_definition(POSTGRES_CONN_ID, dest_table)
    
    if current_view_def:
        current_view_hash = generate_view_hash(current_view_def)
        
        if current_view_hash != new_view_hash:
            logger.info("View definition has changed, recreating view")
            return f"""
                DROP MATERIALIZED VIEW IF EXISTS {dest_table};
                {new_view_sql}
            """
        else:
            logger.info("View definition unchanged, skipping recreation")
            return None
    else:
        logger.info("View does not exist, creating new view")
        return new_view_sql

def generate_materialized_view_sql():
    """Generate SQL for creating/replacing materialized view"""
    logger.info("Starting to generate materialized view SQL")
    
    mapping = load_mapping()
    
    source_table = mapping['source_table']
    dest_table = mapping['destination_table']
    column_mappings = mapping['column_mappings']
    
    # Create column mappings where source_col is used in SELECT and dest_col is the alias
    select_columns = []
    for dest_col, source_col in column_mappings.items():
        select_columns.append(f"{source_col} as {dest_col}")
    
    columns_sql = ",\n    ".join(select_columns)
    
    sql = f"""
    CREATE MATERIALIZED VIEW IF NOT EXISTS {dest_table} AS
    SELECT 
    {columns_sql}
    FROM {source_table};
    """
    
    logger.info("Generated SQL for materialized view:")
    logger.info(sql)
    
    return sql

def generate_refresh_sql():
    """Generate SQL for refreshing the materialized view"""
    logger.info("Starting to generate refresh SQL")
    
    mapping = load_mapping()
    refresh_sql = f"REFRESH MATERIALIZED VIEW {mapping['destination_table']};"
    
    logger.info(f"Generated refresh SQL: {refresh_sql}")
    
    return refresh_sql

# Create the DAG
dag = DAG(
    'materialized_view_game_statistics',
    default_args=default_args,
    description='Creates and refreshes materialized view for game statistics',
    schedule_interval='@daily',
    catchup=False
)

logger.info("Creating DAG: materialized_view_game_statistics")

# Task to create or update materialized view if not exists
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
create_or_update_mv >> refresh_mv

logger.info("DAG setup completed") 