from qa import QueryInsights
from qd import QueryDecomposer
import yaml
import psycopg2
from psycopg2.extras import RealDictCursor
import pandas as pd
from rich.console import Console
from rich.table import Table
from rich import print as rprint
from datetime import datetime
import os
import re

class QueryAnalyzer:
    def __init__(self, db_config_path):
        self.db_config_path = db_config_path
        self.db_config = self._load_db_config()
        self.connections = self._establish_connections()
        self.console = Console()
        self.output_dir = "query_results"
        
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
    
    def _load_db_config(self):
        try:
            with open(self.db_config_path, 'r') as file:
                return yaml.safe_load(file)
        except Exception as e:
            rprint(f"[red]Error loading database config: {str(e)}[/red]")
            return None
    
    def _establish_connections(self):
        """Establish connections to all databases in config."""
        connections = {}
        if not self.db_config:
            return connections
            
        for db in self.db_config.get('databases', []):
            try:
                conn = psycopg2.connect(
                    dbname=db['dbname'],
                    user=db['username'],
                    password=db['password'],
                    host=db['host'],
                    port=db['port']
                )
                connections[db['name']] = conn
            except Exception as e:
                rprint(f"[red]Failed to connect to {db['name']}: {str(e)}[/red]")
        
        return connections
    
    def _get_db_for_table(self, table_name):
        """Find which database contains the given table."""
        # Check if it's a materialized view
        if table_name.lower().startswith(('mv_', 'mv')):
            return 'DB3'
        
        for db in self.db_config.get('databases', []):
            for table in db.get('tables', []):
                if table['name'] == table_name:
                    return db['name']
        return None

    def execute_subquery(self, table_name, subquery):
        """Execute a subquery and return results as a pandas DataFrame."""
        db_name = self._get_db_for_table(table_name)
        if not db_name or db_name not in self.connections:
            return pd.DataFrame(), f"Error: Could not find database for table {table_name}"
            
        conn = self.connections[db_name]
        try:
            return pd.read_sql_query(subquery, conn), None
        except Exception as e:
            return pd.DataFrame(), f"Error executing query: {str(e)}"

    def display_results(self, df, title="Results"):
        """Display DataFrame results using rich table."""
        if df.empty:
            rprint(f"[yellow]No results found for {title}[/yellow]")
            return
            
        table = Table(title=title, show_header=True, header_style="bold magenta")
        
        # Add columns
        for column in df.columns:
            table.add_column(str(column))
        
        # Show only first 10 rows in console
        preview_df = df.head(10)
        for _, row in preview_df.iterrows():
            table.add_row(*[str(value) for value in row])
        
        self.console.print(table)
        if len(df) > 10:
            rprint(f"[yellow]Showing 10 of {len(df)} results[/yellow]")

    def analyze_and_decompose_query(self, query):
        self.console.rule("[bold blue]Query Analysis")
        rprint(f"[green]Original Query:[/green]")
        rprint(query)
        
        # Get query insights
        analyzer = QueryInsights(query, self.db_config)
        self.analysis = analyzer.get_detailed_analysis()
        
        # Check if query has JOINs
        has_joins = bool(self.analysis['joins'])
        
        if not has_joins:
            # For simple queries without JOINs, use direct psycopg2 execution
            result_df = self._execute_simple_query(query)
            if isinstance(result_df, pd.DataFrame):
                self.final_results = result_df
                return result_df
        else:
            # For queries with JOINs, use the existing DataFrame approach
            return self._execute_complex_query(query)

    def _execute_simple_query(self, query):
        """Execute a simple query directly using psycopg2."""
        if not self.analysis['tables_involved']:
            rprint("[red]Error: No tables found in query[/red]")
            return None
        
        first_table = self.analysis['tables_involved'][0]['name']
        
        # Check if it's a materialized view query
        is_mv = first_table.lower().startswith(('mv_', 'mv'))
        
        # For materialized views, always use DB3
        if is_mv:
            db_name = 'DB3'
        else:
            db_name = self._get_db_for_table(first_table)
        
        if not db_name or db_name not in self.connections:
            rprint(f"[red]Error: Could not find database for table {first_table}[/red]")
            return None
        
        conn = self.connections[db_name]
        try:
            # If it's SELECT *, get the column names first for better display
            is_select_all = False
            if 'SELECT *' in query.upper():
                is_select_all = True
                # Get column names
                with conn.cursor() as cursor:
                    cursor.execute(f"""
                        SELECT column_name 
                        FROM information_schema.columns 
                        WHERE table_name = '{first_table}'
                        ORDER BY ordinal_position
                    """)
                    columns = [row[0] for row in cursor.fetchall()]
                    rprint(f"[cyan]Columns in {first_table}: {columns}[/cyan]")
            
            # Execute the actual query
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query)
                results = cursor.fetchall()
                
                # Convert results to DataFrame for display
                df = pd.DataFrame(results)
                
                if is_select_all and not df.empty and columns:
                    df = df.reindex(columns=columns)
                
                self.console.rule("[bold blue]Query Results")
                self.display_results(df, "Query Results")
                
                # Save results to file
                self.save_results_to_file(query, self.analysis, {first_table: query}, df)
                
                # Store the final results for the web interface
                self.final_results = df
                
                # Return the DataFrame for the web interface
                return df
                
        except Exception as e:
            rprint(f"[red]Error executing query: {str(e)}[/red]")
            return None

    def _execute_complex_query(self, query):
        """Execute a complex query with JOINs using the DataFrame approach."""
        decomposer = QueryDecomposer(query, self.db_config)
        subqueries = decomposer.generate_table_subqueries()
        join_conditions = decomposer.get_join_conditions()
        
        # Store DataFrames for each table
        dataframes = {}
        error_messages = []
        self.subquery_results = {}  # Store subquery results
        
        rprint("\n[green]Executing Subqueries:[/green]")
        for table, subquery in subqueries.items():
            rprint(f"\n[yellow]{table}:[/yellow]")
            rprint(subquery)
            df, error = self.execute_subquery(table, subquery)
            if error:
                error_messages.append(error)
            else:
                # Convert numeric columns for this table
                df = self._convert_numeric_columns(df, table)
                dataframes[table] = df
                # Store subquery results
                self.subquery_results[table] = {
                    'query': subquery,
                    'results': df,
                    'columns': df.columns.tolist(),
                    'total_rows': len(df)
                }
                self.display_results(df, f"Results for {table}")

        if error_messages:
            for error in error_messages:
                rprint(f"[red]{error}[/red]")
            return

        # Perform joins based on analysis
        result_df = self.perform_joins(dataframes, self.analysis['joins'])
        
        # Apply WHERE conditions if present
        if 'WHERE' in query.upper():
            where_start = query.upper().find('WHERE')
            where_end = len(query)
            for keyword in ['GROUP BY', 'ORDER BY', 'HAVING', 'LIMIT']:
                pos = query.upper().find(keyword)
                if pos != -1 and pos < where_end:
                    where_end = pos
            
            where_clause = query[where_start + 5:where_end].strip()
            where_clause = self.convert_sql_where_to_pandas(where_clause)
            rprint(f"[cyan]Applying WHERE clause: {where_clause}[/cyan]")
            try:
                result_df = self._convert_numeric_columns(result_df)
                result_df = result_df.query(where_clause)
                rprint(f"[green]WHERE clause applied successfully. {len(result_df)} rows remaining.[/green]")
            except Exception as e:
                rprint(f"[red]Error applying WHERE clause: {str(e)}[/red]")
                rprint(f"[yellow]Available columns: {result_df.columns.tolist()}[/yellow]")
                rprint(f"[yellow]Column types: {result_df.dtypes.to_dict()}[/yellow]")

        # Apply GROUP BY if present
        group_match = re.search(r'\bGROUP\s+BY\s+([^;]+?)(?:\s+(?:HAVING|ORDER|LIMIT)|;|$)', query, re.IGNORECASE | re.DOTALL)
        if group_match:
            group_cols = [col.strip() for col in group_match.group(1).split(',')]
            # Remove table aliases from group columns
            group_cols = [col.split('.')[-1].strip() for col in group_cols]
            
            # Find aggregation functions in SELECT clause
            select_clause = query.upper().split('FROM')[0].replace('SELECT', '').strip()
            agg_pattern = r'(COUNT|SUM|AVG|MAX|MIN)\s*\(\s*\*?\s*\)(?:\s+AS\s+([a-zA-Z0-9_]+))?'
            agg_matches = re.finditer(agg_pattern, select_clause, re.IGNORECASE)
            
            # Handle aggregations
            agg_funcs = {}
            agg_aliases = {}  # Store original aliases
            for match in agg_matches:
                agg_func = match.group(1).lower()
                alias = match.group(2)
                if alias:
                    # Store the original alias case
                    original_alias = re.search(rf'\b{alias}\b', query, re.IGNORECASE).group(0)
                    agg_aliases[alias.lower()] = original_alias
                    # If there's an alias, use it as the column name
                    if agg_func == 'count':
                        agg_funcs['size'] = original_alias  # Use size() for COUNT(*)
                    else:
                        agg_funcs[agg_func] = original_alias
                else:
                    # If no alias, use the function name
                    if agg_func == 'count':
                        agg_funcs['size'] = f'{agg_func}_star'
                    else:
                        agg_funcs[agg_func] = f'{agg_func}_result'
            
            # Apply groupby with aggregation
            if agg_funcs:
                # For COUNT(*), we need special handling
                if 'size' in agg_funcs:
                    # Group by the columns and get the size
                    result_df = result_df.groupby(group_cols, as_index=False).size()
                    # Rename the 'size' column to the specified alias
                    result_df = result_df.rename(columns={'size': agg_funcs['size']})
                else:
                    # For other aggregations
                    result_df = result_df.groupby(group_cols, as_index=False).agg(agg_funcs)
                
                rprint(f"[green]Applied GROUP BY on {group_cols} with aggregations {agg_funcs}[/green]")
            else:
                result_df = result_df.groupby(group_cols, as_index=False).size()
                rprint(f"[green]Applied GROUP BY on {group_cols}[/green]")

        # Apply ORDER BY if present
        order_match = re.search(r'\bORDER\s+BY\s+([^;]+?)(?:\s+LIMIT|;|$)', query, re.IGNORECASE | re.DOTALL)
        if order_match:
            order_cols = []
            ascending = []
            
            for item in order_match.group(1).split(','):
                item = item.strip()
                if ' DESC' in item.upper():
                    col = item.replace(' DESC', '').replace(' desc', '').strip()
                    col = col.split('.')[-1].strip()  # Remove table alias
                    # Try to match case-insensitive column name
                    col_lower = col.lower()
                    if col_lower in [c.lower() for c in result_df.columns]:
                        actual_col = next(c for c in result_df.columns if c.lower() == col_lower)
                        order_cols.append(actual_col)
                        ascending.append(False)
                else:
                    col = item.replace(' ASC', '').replace(' asc', '').strip()
                    col = col.split('.')[-1].strip()  # Remove table alias
                    # Try to match case-insensitive column name
                    col_lower = col.lower()
                    if col_lower in [c.lower() for c in result_df.columns]:
                        actual_col = next(c for c in result_df.columns if c.lower() == col_lower)
                        order_cols.append(actual_col)
                        ascending.append(True)
            
            try:
                if order_cols:  # Only proceed if we have columns to sort by
                    result_df = result_df.sort_values(by=order_cols, ascending=ascending)
                    direction_str = 'DESC' if ascending and not ascending[0] else 'ASC'
                    rprint(f"[green]Applied ORDER BY {order_cols} {direction_str}[/green]")
                else:
                    rprint("[yellow]Warning: No valid columns found for ORDER BY[/yellow]")
                    rprint(f"[yellow]Available columns: {result_df.columns.tolist()}[/yellow]")
            except KeyError as e:
                rprint(f"[red]Error in ORDER BY: Column {e} not found[/red]")
                rprint(f"[yellow]Available columns: {result_df.columns.tolist()}[/yellow]")
            except Exception as e:
                rprint(f"[red]Error applying ORDER BY: {str(e)}[/red]")
                rprint(f"[yellow]Available columns: {result_df.columns.tolist()}[/yellow]")

        # Apply LIMIT at the end if present
        limit_match = re.search(r'\bLIMIT\s+(\d+)', query, re.IGNORECASE)
        if limit_match:
            limit_value = int(limit_match.group(1))
            result_df = result_df.head(limit_value)
            rprint(f"[green]Applied LIMIT {limit_value}[/green]")
        
        self.console.rule("[bold blue]Final Results")
        self.display_results(result_df, "Final Combined Results")
        
        # Save results to file
        self.save_results_to_file(query, self.analysis, subqueries, result_df)
        
        # Store the final results
        self.final_results = result_df

    def perform_joins(self, dataframes, joins):
        """Perform joins between DataFrames based on join conditions."""
        if not joins or len(dataframes) < 2:
            return next(iter(dataframes.values()))
        
        result_df = None
        processed_tables = set()
        
        # Create a mapping of aliases to actual table names
        alias_to_table = {
            table['alias']: table['name'] 
            for table in self.analysis['tables_involved']
        }
        
        for join in joins:
            # Parse join condition
            condition = join['condition']
            left_alias, left_col = condition.split('=')[0].strip().split('.')
            right_alias, right_col = condition.split('=')[1].strip().split('.')
            
            # Convert aliases to actual table names
            left_table = alias_to_table[left_alias.lower()]
            right_table = alias_to_table[right_alias.lower()]
            
            # Clean up column names and convert to lowercase
            left_col = left_col.strip().lower()
            right_col = right_col.strip().lower()
            
            if result_df is None:
                # First join
                # Print column names and data for debugging
                left_df = dataframes[left_table]
                right_df = dataframes[right_table]
                
                rprint(f"[cyan]Left DataFrame columns: {left_df.columns.tolist()}[/cyan]")
                rprint(f"[cyan]Right DataFrame columns: {right_df.columns.tolist()}[/cyan]")
                
                # Convert column names to lowercase for comparison
                left_df.columns = left_df.columns.str.lower()
                right_df.columns = right_df.columns.str.lower()
                
                rprint(f"[cyan]Joining on left column '{left_col}' and right column '{right_col}'[/cyan]")
                rprint(f"[cyan]Left unique values: {left_df[left_col].unique().tolist()[:5]}...[/cyan]")
                rprint(f"[cyan]Right unique values: {right_df[right_col].unique().tolist()[:5]}...[/cyan]")
                
                result_df = pd.merge(
                    left_df,
                    right_df,
                    left_on=left_col,
                    right_on=right_col,
                    how=self.get_join_type(join['type'])
                )
                
                rprint(f"[cyan]Merged DataFrame has {len(result_df)} rows[/cyan]")
                
                processed_tables.add(left_table)
                processed_tables.add(right_table)
            else:
                # Subsequent joins
                if right_table not in processed_tables:
                    right_df = dataframes[right_table]
                    right_df.columns = right_df.columns.str.lower()
                    
                    result_df = pd.merge(
                        result_df,
                        right_df,
                        left_on=left_col,
                        right_on=right_col,
                        how=self.get_join_type(join['type'])
                    )
                    processed_tables.add(right_table)
        
        return result_df

    def get_join_type(self, sql_join_type):
        """Convert SQL join type to pandas merge type."""
        join_map = {
            'INNER JOIN': 'inner',
            'LEFT JOIN': 'left',
            'RIGHT JOIN': 'right',
            'FULL JOIN': 'outer',
            'JOIN': 'inner'  # Default to inner join
        }
        return join_map.get(sql_join_type.upper(), 'inner')

    def convert_sql_where_to_pandas(self, where_clause):
        """Convert SQL WHERE clause to pandas query syntax."""
        # Store string literals with their original case
        string_literals = {}
        literal_count = 0
        string_pattern = r"'([^']*)'"
        
        def replace_literal(match):
            nonlocal literal_count
            literal = match.group(1)
            placeholder = f"__STRING_LITERAL_{literal_count}__"
            string_literals[placeholder] = literal
            literal_count += 1
            return placeholder
        
        # Replace string literals with placeholders
        where_clause = re.sub(string_pattern, replace_literal, where_clause)
        
        # Remove table aliases from the where clause
        # Look for patterns like "alias.column" and replace with just "column"
        alias_pattern = r'([a-zA-Z_][a-zA-Z0-9_]*)\.'
        where_clause = re.sub(alias_pattern, '', where_clause)
        
        # Convert column names and operators to lowercase
        where_clause = where_clause.lower()
        
        # Basic conversions
        where_clause = where_clause.replace(' and ', ' & ')
        where_clause = where_clause.replace(' or ', ' | ')
        where_clause = where_clause.replace(' = ', ' == ')
        where_clause = where_clause.replace('<>', '!=')
        
        # Handle LIKE operator
        if ' like ' in where_clause:
            # Replace LIKE with str.contains() for pandas
            like_parts = where_clause.split(' like ')
            column = like_parts[0].strip()
            pattern = like_parts[1].strip()
            
            # Get the original string literal
            for placeholder, original in string_literals.items():
                if placeholder.lower() in pattern.lower():
                    # Convert SQL LIKE pattern to regex pattern
                    pattern = original.replace('%', '.*').replace('_', '.')
                    # Reconstruct where clause using str.contains()
                    where_clause = f"{column}.str.contains('{pattern}', case=False, regex=True, na=False)"
                    break
        else:
            # Restore string literals with their original case for non-LIKE conditions
            for placeholder, original in string_literals.items():
                where_clause = where_clause.replace(placeholder.lower(), f"'{original}'")
        
        return where_clause

    def save_results_to_file(self, query, analysis, subqueries, final_df):
        """Save analysis and results to a markdown file."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{self.output_dir}/query_analysis_{timestamp}.md"
        
        with open(filename, 'w') as f:
            f.write("# Query Analysis Report\n\n")
            f.write("## Original Query\n```sql\n")
            f.write(query.strip() + "\n```\n\n")
            
            f.write("## Query Analysis\n")
            for key, value in analysis.items():
                f.write(f"\n### {key.replace('_', ' ').title()}\n")
                f.write(f"{value}\n")
            
            f.write("\n## Subqueries\n")
            for table, subquery in subqueries.items():
                f.write(f"\n### {table}\n```sql\n")
                f.write(subquery + "\n```\n")
            
            f.write("\n## Results Preview\n")
            f.write(final_df.head(10).to_markdown())
            
            if len(final_df) > 10:
                f.write(f"\n\n*Showing 10 of {len(final_df)} results*\n")
        
        rprint(f"[green]Results saved to: {filename}[/green]")

    def __del__(self):
        """Close all database connections."""
        for conn in self.connections.values():
            try:
                conn.close()
            except:
                pass

    def _parse_selected_columns(self):
        """Parse the selected columns from the query."""
        parsed_columns = []
        select_clause = self.query.upper().split('FROM')[0].replace('SELECT', '').strip()
        
        for col in select_clause.split(','):
            col = col.strip()
            alias = None
            if ' AS ' in col:
                col, alias = [p.strip() for p in col.split(' AS ')]
            parsed_columns.append({'column': col, 'alias': alias})
        
        return parsed_columns

    def _convert_numeric_columns(self, df, table_name=None):
        """Convert numeric columns to appropriate types."""
        # List of known numeric columns (add more as needed)
        numeric_columns = {
            'steam_games': ['estimated_owners', 'price', 'rating', 'required_age'],
            'modders': ['followers', 'downloads'],
            # Add more tables and their numeric columns as needed
        }
        
        try:
            # If table_name is provided, only convert columns for that table
            if table_name and table_name in numeric_columns:
                cols_to_convert = numeric_columns[table_name]
                for col in cols_to_convert:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
            else:
                # If no table_name provided, try to convert all known numeric columns
                for table_cols in numeric_columns.values():
                    for col in table_cols:
                        if col in df.columns:
                            df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Also try to convert any column that looks numeric
            for col in df.columns:
                if col.lower().endswith(('_count', '_amount', '_size', '_number', '_qty', '_id', '_owners')):
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                
        except Exception as e:
            rprint(f"[yellow]Warning converting numeric columns: {str(e)}[/yellow]")
        
        return df

if __name__ == "__main__":
    analyzer = QueryAnalyzer('config.yaml')
    query = """
SELECT 
    game_genres,
    player_country,
    COUNT(DISTINCT player_id) AS total_players,
    COUNT(DISTINCT game_developers) AS active_developers
FROM 
    mv_game_player_content_region
WHERE 
    player_country = 'India'
GROUP BY 
    game_genres, player_country
ORDER BY 
    total_players DESC, active_developers DESC;
"""
    analyzer.analyze_and_decompose_query(query)
    