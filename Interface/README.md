# Interface Module

This module provides a web interface for the Arkaid project, featuring natural language to SQL query conversion powered by TogetherAI and interactive data visualization capabilities.

## Overview

The Interface module offers:
- Natural language to SQL query conversion
- Interactive query execution and visualization
- Database connection management
- Query analysis and decomposition
- Performance monitoring and optimization

## Components

### Core Components
- `app.py`: Main Flask application and web server
- `ai.py`: TogetherAI integration for natural language processing
- `qa.py`: Query analysis and insights functionality
- `qd.py`: Query decomposition and optimization logic

### Configuration
- `config.yaml`: Main configuration file for database and application settings
- `.env.example`: Environment variables template for sensitive credentials
- `prompt.txt`: AI system prompt configuration for query generation
- `db_config_generator.py`: Automated database configuration generator

### Testing & Utilities
- `connection_tester.py`: Database connection testing utility
- `example_usage.py`: Example implementations and usage patterns

### Web Interface
- `templates/`: HTML templates for web pages
  - `index.html`: Main landing page
  - `query.html`: Query execution and results page
  - `ai.html`: Natural language query interface
- `static/`: CSS and static assets
  - `styles.css`: Main stylesheet
  - `ai.css`: AI interface styling
- `queries_p1/`: Predefined query examples and templates

## Setup

1. Environment Configuration:
```bash
cp .env.example .env
# Edit .env with your credentials and API keys
```

2. Install Dependencies:
```bash
pip install -r requirements.txt
```

3. Configure Database:
```bash
python db_config_generator.py
```

4. Test Connections:
```bash
python connection_tester.py
```

## Usage

### Starting the Application
```bash
python app.py
```
The application will be available at `http://localhost:4321`

### Using Natural Language Queries
1. Navigate to the AI interface
2. Enter your query in natural language
3. Review the generated SQL
4. Execute the query and view results

### Using Predefined Queries
1. Browse available queries in the main interface
2. Select a query to view details and execution plan
3. Execute the query and analyze results

## Features

### Natural Language Processing
- Converts English queries to SQL using TogetherAI
- Provides query explanations and insights
- Handles complex analytical queries
- Supports multiple database sources

### Query Management
- Query analysis and optimization
- Performance monitoring
- Result visualization
- Error handling and debugging

### Database Integration
- Supports multiple database connections
- Handles materialized views
- Manages complex joins and subqueries
- Provides query decomposition

## Dependencies

### Python Packages
- Flask
- psycopg2
- pandas
- PyYAML
- python-dotenv
- sqlanalyzer
- SQLAlchemy
- rich
- openai

### External Services
- TogetherAI API for natural language processing
- PostgreSQL databases

## Notes

- The interface runs on port 4321 by default
- Natural language processing requires a valid TogetherAI API key
- Query execution is optimized for materialized views when possible
- The interface includes built-in error handling and user feedback
- All database operations are executed with appropriate security measures
