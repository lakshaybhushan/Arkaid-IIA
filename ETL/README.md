# ETL Module

This module handles the Extract, Transform, Load (ETL) processes for the Arkaid project, managing data flow between different databases and creating materialized views for efficient analytics.

## Overview

The ETL module is responsible for:
- Extracting data from multiple sources
- Transforming data into standardized formats
- Loading data into appropriate databases
- Creating and maintaining materialized views
- Managing database connections and configurations

## Components

### Database Connection Management
- `db_connection.py`: Manages database connections
- `db_config.yaml`: Configuration file for database connections
- `.env.example`: Template for environment variables

### ETL Processes
- `etl_steam_games.py`: ETL process for Steam games data
- `etl_epic_games.py`: ETL process for Epic games data
- `etl_steam_players.py`: ETL process for Steam player data
- `etl_epic_players.py`: ETL process for Epic player data
- `etl_content_creators.py`: ETL process for content creator data
- `etl_developers.py`: ETL process for developer data
- `etl_modders.py`: ETL process for modder data
- `etl_publishers.py`: ETL process for publisher data

### Materialized Views
- `mv_games_warehouse.py`: Manages game-related materialized views
- `mv_players_warehouse.py`: Manages player-related materialized views

### Schema Management
- `schema_matcher.py`: Handles schema matching between different data sources
- `other_schema_matcher.py`: Additional schema matching functionality
- `csv_data_sources.py`: Manages CSV data source configurations

## Directory Structure

```
ETL/
├── data/                  # Data files directory
├── mappings/             # Schema mapping configurations
├── .env.example          # Environment variables template
├── db_config.yaml        # Database configuration
├── data_types.txt        # Data type definitions
└── ETL process files     # Individual ETL scripts
```

## Configuration

1. Copy `.env.example` to `.env` and fill in database credentials:
```bash
cp .env.example .env
```

2. Update `db_config.yaml` with appropriate database configurations:
```yaml
databases:
  - name: DB1
    host: your_host
    port: "5432"
    dbname: your_db
    username: your_username
    password: your_password
```

## Usage

### Running ETL Processes

1. For Steam data:
```bash
python etl_steam_games.py
python etl_steam_players.py
```

2. For Epic data:
```bash
python etl_epic_games.py
python etl_epic_players.py
```

3. For other entities:
```bash
python etl_content_creators.py
python etl_developers.py
python etl_modders.py
python etl_publishers.py
```

### Managing Materialized Views

1. Create/update game-related views:
```bash
python mv_games_warehouse.py
```

2. Create/update player-related views:
```bash
python mv_players_warehouse.py
```

## Database Structure

The ETL processes work with three main databases:
1. DB1: Epic Games data
2. DB2: Steam data
3. DB3: Materialized views and consolidated data

## Dependencies

- Python 3.x
- psycopg2
- pandas
- PyYAML
- python-dotenv
- SQLAlchemy

## Notes

- ETL processes include error handling and logging
- Materialized views are automatically refreshed when source data changes
- Schema matching ensures data consistency across different sources
- All processes are designed to be idempotent
