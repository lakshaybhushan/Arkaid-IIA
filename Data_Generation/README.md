# Data Generation Module

This module is responsible for generating and managing data for the Arkaid project, a game performance analytical tool that utilizes data warehouse approaches and integrates data from multiple sources.

## Overview

The Data Generation module handles the creation, transformation, and management of various gaming-related datasets including:
- Game data (Steam and Epic platforms)
- Player information
- Developer and publisher details
- Content creator and modder data
- Game statistics and performance metrics

## Key Components

### Game Data Generation
- `extract_steam_games.py`: Extracts and processes Steam games data
- `update_steam_games.py`: Updates Steam games with additional metrics
- `update_epic_games.py`: Manages Epic games data updates
- `update_epic_ids.py`: Handles Epic game ID management
- `generate_game_stats.py`: Creates comprehensive game statistics

### Player Data Management
- `generate_players.py`: Creates player profiles and data
- `generate_steam_players.py`: Generates Steam-specific player data
- `generate_epic_players.py`: Creates Epic Games player profiles
- `generate_player_stats.py`: Generates player statistics and metrics
- `generate_player_sales.py`: Creates player purchase and sales data

### Content Creator & Modder Data
- `generate_content_creators.py`: Generates content creator profiles
- `generate_modders.py`: Creates modder data and associations
- `update_creators_modders.py`: Updates content creator and modder information

### Developer & Publisher Data
- `devs.py`: Manages developer data generation
- `pub.py`: Handles publisher data creation
- `filter_dev_pub.py`: Filters and processes developer/publisher data

### Data Maintenance
- `move_recently_played.py`: Manages recently played game data
- `remove_library_columns.py`: Handles library column management
- `reorder_columns.py`: Manages column ordering in datasets
- `split_players_table.py`: Splits player data into normalized tables

## Usage

Each script can be run independently to generate or update specific datasets. The general workflow is:

1. Generate base game data:
```bash
python extract_steam_games.py
python update_epic_games.py
```

2. Create player profiles:
```bash
python generate_players.py
python generate_steam_players.py
python generate_epic_players.py
```

3. Generate additional data:
```bash
python generate_game_stats.py
python generate_content_creators.py
python generate_modders.py
```

4. Update and maintain data:
```bash
python update_creators_modders.py
python update_game_stats.py
python update_player_ratings.py
```

## Data Types

The module handles various data types including:
- Text data for names, descriptions, and identifiers
- Numeric data for statistics and metrics
- Date/time data for temporal information
- Boolean flags for status indicators
- Arrays and complex data structures for related information

## Dependencies

- Python 3.x
- pandas
- numpy
- faker
- tqdm
- psycopg2
- pycountry
- jellyfish

## Notes

- All generated data follows specific patterns and distributions to maintain realism
- Data generation takes into account relationships between different entities
- Scripts include error handling and validation to ensure data integrity
- Generated data is used to populate the data warehouse for analytics
