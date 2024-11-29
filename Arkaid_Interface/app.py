from flask import Flask, render_template, request, jsonify
from example_usage import QueryAnalyzer
import yaml
import pandas as pd
import time
import os

app = Flask(__name__)

def load_queries_from_files():
    """Load queries from files - called on each request to ensure fresh data"""
    queries = {}
    queries_dir = 'queries_p1'
    
    # Ensure the queries directory exists
    if not os.path.exists(queries_dir):
        os.makedirs(queries_dir)
    
    # Read all .txt files from the queries directory
    for filename in os.listdir(queries_dir):
        if filename.endswith('.txt'):
            query_id = filename[:-4]  # Remove .txt extension
            file_path = os.path.join(queries_dir, filename)
            
            with open(file_path, 'r') as file:
                content = file.read().strip().split('\n---\n')  # Split by separator
                
                # Extract title, description, and query from file content
                title = content[0].strip()
                description = content[1].strip()
                query = content[2].strip()
                
                queries[query_id] = {
                    'title': title,
                    'description': description,
                    'query': query
                }
    
    return queries

@app.route('/')
def index():
    # Load queries fresh on each request
    queries = load_queries_from_files()
    return render_template('index.html', queries=queries)

@app.route('/query/<query_id>')
def show_query(query_id):
    # Load queries fresh on each request
    queries = load_queries_from_files()
    
    if query_id not in queries:
        return "Query not found", 404
    
    query_info = queries[query_id]
    
    # Load config and create analyzer
    with open('config.yaml', 'r') as file:
        db_config = yaml.safe_load(file)
    
    analyzer = QueryAnalyzer('config.yaml')
    
    # Time the query execution
    start_time = time.time()
    
    try:
        # Analyze and execute the query
        result = analyzer.analyze_and_decompose_query(query_info['query'])
        
        # Calculate execution time
        execution_time = round(time.time() - start_time, 3)
        
        # Initialize results
        final_results = None
        subquery_results = {}
        
        # Handle simple queries (no subqueries)
        if isinstance(result, pd.DataFrame):
            final_results = {
                'data': result.to_dict('records')[:10],
                'columns': result.columns.tolist(),
                'total_rows': len(result)
            }
        # Handle complex queries with subqueries
        elif hasattr(analyzer, 'final_results'):
            final_results = {
                'data': analyzer.final_results.to_dict('records')[:10],
                'columns': analyzer.final_results.columns.tolist(),
                'total_rows': len(analyzer.final_results)
            }
            
        # Get subquery results if available
        if hasattr(analyzer, 'subquery_results'):
            for table, data in analyzer.subquery_results.items():
                subquery_results[table] = {
                    'query': data['query'],
                    'results': data['results'].to_dict('records')[:10],
                    'columns': data['columns'],
                    'total_rows': data['total_rows']
                }
        
        print("Final Results:", final_results)  # Debug print
        print("Subquery Results:", subquery_results)  # Debug print
        
        return render_template('query.html',
                             query_id=query_id,
                             query_info=query_info,
                             final_results=final_results,
                             subquery_results=subquery_results,
                             execution_time=execution_time,
                             queries=queries)
                             
    except Exception as e:
        print(f"Error executing query: {str(e)}")  # Debug print
        return f"Error executing query: {str(e)}", 500

if __name__ == '__main__':
    app.run(debug=True, port=3000) 