from flask import Flask, render_template, request, jsonify, redirect, url_for
from example_usage import QueryAnalyzer
import yaml
import pandas as pd
import time
import os
from ai import generate_sql_query, generate_fun_fact
import json

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

def execute_sql_query(sql_query):
    """Execute a SQL query and return the results"""
    try:
        # Create analyzer instance using config
        analyzer = QueryAnalyzer('config.yaml')
        
        # Execute the query
        result = analyzer.analyze_and_decompose_query(sql_query)
        
        # Handle the result
        if isinstance(result, pd.DataFrame):
            return {
                'data': result.to_dict('records')[:10],  # Limit to first 10 rows
                'columns': result.columns.tolist(),
                'total_rows': len(result)
            }
        elif hasattr(analyzer, 'final_results'):
            return {
                'data': analyzer.final_results.to_dict('records')[:10],
                'columns': analyzer.final_results.columns.tolist(),
                'total_rows': len(analyzer.final_results)
            }
        else:
            raise Exception("No results returned from query execution")
            
    except Exception as e:
        raise Exception(f"Error executing query: {str(e)}")

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

@app.route('/ai')
def ai_page():
    """Render the AI SQL query generator page"""
    return render_template('ai.html')

@app.route('/generate-query', methods=['POST'])
def generate_query():
    """Generate SQL query from natural language input"""
    user_input = request.form.get('user_input')
    if not user_input:
        return render_template('ai.html', error="Please provide input text")
    
    sql_query = generate_sql_query(user_input)
    if not sql_query:
        return render_template('ai.html', 
                             error="Failed to generate SQL query",
                             user_input=user_input)  # Keep the user input
    
    return render_template('ai.html', 
                         sql_query=sql_query,
                         user_input=user_input)  # Keep the user input

@app.route('/execute-query', methods=['POST'])
def execute_query():
    """Execute the generated SQL query"""
    sql_query = request.form.get('sql_query')
    user_input = request.form.get('user_input')
    
    if not sql_query:
        return render_template('ai.html', 
                             error="No SQL query provided",
                             user_input=user_input)
    
    try:
        results = execute_sql_query(sql_query)
        return render_template('ai.html', 
                             sql_query=sql_query,
                             results=results,
                             user_input=user_input)
    except Exception as e:
        return render_template('ai.html', 
                             sql_query=sql_query,
                             error=f"Error executing query: {str(e)}",
                             user_input=user_input)

@app.route('/generate_fun_fact', methods=['POST'])
def generate_fun_fact_route():
    """Generate a fun fact based on query results"""
    try:
        results_data = request.form.get('results_data')
        sql_query = request.form.get('sql_query')
        user_input = request.form.get('user_input')
        
        if results_data:
            # Parse the JSON data
            results_data = json.loads(results_data)
            
            # Generate the fun fact
            fun_fact = generate_fun_fact(results_data[:3])  # Use first 3 results for context
            
            # Return the template with all necessary data
            return render_template('ai.html', 
                                results={"data": results_data, "columns": list(results_data[0].keys())}, 
                                fun_fact=fun_fact,
                                sql_query=sql_query,
                                user_input=user_input)
    except Exception as e:
        print(f"Error generating fun fact: {str(e)}")
        return render_template('ai.html', 
                             error=f"Error generating fun fact: {str(e)}",
                             sql_query=sql_query,
                             user_input=user_input)
    
    return redirect(url_for('index'))

if __name__ == '__main__':
    # Make sure all required files exist
    if not os.path.exists('templates/ai.html'):
        raise FileNotFoundError("Missing templates/ai.html")
    if not os.path.exists('static/ai.css'):
        raise FileNotFoundError("Missing static/ai.css")
    if not os.path.exists('config.yaml'):
        raise FileNotFoundError("Missing config.yaml")
        
    app.run(debug=True, port=4321) 