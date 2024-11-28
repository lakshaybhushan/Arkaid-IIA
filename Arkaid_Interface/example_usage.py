from qa import QueryInsights
from qd import QueryDecomposer
import yaml
import psycopg2
from psycopg2.extras import RealDictCursor
from rich.console import Console
from rich.table import Table
from rich import print as rprint
from datetime import datetime
import os

class QueryAnalyzer:
    def __init__(self, db_config_path):
        self.db_config_path = db_config_path
        self.db_config = self._load_db_config()
        self.connections = self._establish_connections()
        self.console = Console()
        self.output_dir = "query_results"
        
        # Create output directory if it doesn't exist
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
        for db in self.db_config.get('databases', []):
            for table in db.get('tables', []):
                if table['name'] == table_name:
                    return db['name']
        return None

    def execute_subquery(self, table_name, subquery):
        """Execute a subquery on the appropriate database."""
        db_name = self._get_db_for_table(table_name)
        if not db_name or db_name not in self.connections:
            return f"Error: Could not find database for table {table_name}"
            
        conn = self.connections[db_name]
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Execute without LIMIT - get all results
                cur.execute(subquery)
                results = cur.fetchall()
                return [dict(row) for row in results]
        except Exception as e:
            return f"Error executing query: {str(e)}"

    def display_results(self, results, table_name):
        """Display query results using rich table."""
        if isinstance(results, str):  # Error message
            rprint(f"[red]{results}[/red]")
            return
            
        if not results:
            rprint(f"[yellow]No results found for {table_name}[/yellow]")
            return
            
        # Create rich table
        table = Table(title=f"Results for {table_name}", show_header=True, header_style="bold magenta")
        
        # Add columns
        for column in results[0].keys():
            table.add_column(column)
        
        # Show only first 10 rows in console
        preview_results = results[:10]
        for row in preview_results:
            table.add_row(*[str(value) for value in row.values()])
        
        self.console.print(table)
        if len(results) > 10:
            rprint(f"[yellow]Showing 10 of {len(results)} results[/yellow]")

    def save_results_to_file(self, query, analysis, subqueries, results_dict, materialized_view_sql):
        """Save analysis and results to a markdown file with limited preview."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{self.output_dir}/query_analysis_{timestamp}.md"
        
        with open(filename, 'w') as f:
            # Write Query
            f.write("# Query Analysis Report\n\n")
            f.write("## Original Query\n")
            f.write("```sql\n")
            f.write(query.strip() + "\n")
            f.write("```\n\n")
            
            # Write Analysis
            f.write("## Query Analysis\n")
            for key, value in analysis.items():
                f.write(f"\n### {key.replace('_', ' ').title()}\n")
                f.write(f"{value}\n")
            
            # Write Subqueries and Results Preview
            f.write("\n## Subqueries and Results Preview\n")
            
            for table, subquery in subqueries.items():
                f.write(f"\n### {table}\n")
                f.write("#### Subquery:\n")
                f.write("```sql\n")
                f.write(f"{subquery}\n")
                f.write("```\n\n")
                
                f.write("#### Results Preview:\n")
                if table in results_dict:
                    results = results_dict[table]
                    if isinstance(results, str):  # Error message
                        f.write(f"Error: {results}\n")
                    elif results:
                        # Write headers
                        headers = results[0].keys()
                        f.write("| " + " | ".join(str(h) for h in headers) + " |\n")
                        f.write("| " + " | ".join("---" for _ in headers) + " |\n")
                        
                        # Write only up to 10 rows
                        for row in results[:10]:
                            row_data = " | ".join(str(value) for value in row.values())
                            f.write(f"| {row_data} |\n")
                        
                        # Add note if there are more results
                        if len(results) > 10:
                            f.write(f"\n*Showing 10 of {len(results)} results*\n")
                    else:
                        f.write("No results found\n")
                f.write("\n")
            
            # Write Materialized View SQL
            f.write("\n## Generated Materialized View\n")
            f.write("```sql\n")
            f.write(materialized_view_sql)
            f.write("\n```\n")
            
            # Add execution instructions
            f.write("\n## Usage Instructions\n")
            f.write("1. To create the materialized view, execute the CREATE MATERIALIZED VIEW statement\n")
            f.write("2. To refresh the data in the view, execute the REFRESH MATERIALIZED VIEW statement\n")
            f.write("3. To query the materialized view:\n")
            f.write("```sql\n")
            f.write(f"SELECT * FROM {materialized_view_sql.split('CREATE MATERIALIZED VIEW IF NOT EXISTS ')[1].split(' ')[0]};\n")
            f.write("```\n")
        
        rprint(f"[green]Results saved to: {filename}[/green]")

    def analyze_and_decompose_query(self, query):
        self.console.rule("[bold blue]Query Analysis")
        rprint(f"[green]Original Query:[/green]")
        rprint(query)
        
        # Get query insights
        analyzer = QueryInsights(query, self.db_config)
        analysis = analyzer.get_detailed_analysis()
        
        self.console.rule("[bold blue]Analysis Results")
        for key, value in analysis.items():
            rprint(f"\n[yellow]{key.replace('_', ' ').title()}:[/yellow]")
            rprint(value)
        
        # Decompose query
        self.console.rule("[bold blue]Query Decomposition")
        
        decomposer = QueryDecomposer(query, self.db_config)
        subqueries = decomposer.generate_table_subqueries()
        join_conditions = decomposer.get_join_conditions()
        
        # Store results for each table
        results_dict = {}
        
        rprint("\n[green]Generated Subqueries:[/green]")
        for table, subquery in subqueries.items():
            rprint(f"\n[yellow]{table}:[/yellow]")
            rprint(subquery)
            rprint("\n[cyan]Results:[/cyan]")
            results = self.execute_subquery(table, subquery)
            results_dict[table] = results
            self.display_results(results, table)
        
        rprint("\n[green]Join Conditions:[/green]")
        for tables, join_info in join_conditions.items():
            rprint(f"\n[yellow]Between {' and '.join(tables)}:[/yellow]")
            rprint(f"Type: {join_info['type']}")
            rprint(f"Condition: {join_info['condition']}")
        
        # Generate materialized view SQL
        materialized_view_sql = self.generate_materialized_view_sql(query, subqueries, join_conditions)
        
        # Print the materialized view SQL
        self.console.rule("[bold blue]Materialized View SQL")
        rprint("[green]Generated Materialized View SQL:[/green]")
        rprint(materialized_view_sql)
        
        # Save all results to file (now including materialized view SQL)
        self.save_results_to_file(query, analysis, subqueries, results_dict, materialized_view_sql)

    def generate_materialized_view_sql(self, query, subqueries, join_conditions):
        """Generate SQL to create or refresh materialized view from subqueries."""
        # Extract view name from original query
        # Use first selected column's table and aggregation type as view name
        analyzer = QueryInsights(query, self.db_config)
        analysis = analyzer.get_detailed_analysis()
        
        # Generate a meaningful view name
        selected_cols = analysis['selected_columns']
        if isinstance(selected_cols, list) and len(selected_cols) > 0:
            # Get aggregation type if exists
            agg_type = None
            for col in selected_cols:
                if any(agg in col.upper() for agg in ('SUM(', 'COUNT(', 'AVG(', 'MAX(', 'MIN(')):
                    agg_type = col.split('(')[0].lower()
                    break
            
            # Get first table name
            first_table = analysis['tables_involved'][0]['name'] if analysis['tables_involved'] else 'query'
            
            # Construct view name
            view_name = f"{agg_type}_{first_table}" if agg_type else f"mv_{first_table}"
        else:
            view_name = "query_result"

        # Generate the CREATE/REFRESH SQL
        create_sql = f"""
-- Drop view if exists (optional)
-- DROP MATERIALIZED VIEW IF EXISTS {view_name};

-- Create materialized view if it doesn't exist
CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name} AS
SELECT {', '.join(selected_cols)}
FROM (
    {subqueries[analysis['tables_involved'][0]['name']]}
) {analysis['tables_involved'][0]['alias']}
"""

        # Add JOIN clauses
        for i, table in enumerate(analysis['tables_involved'][1:], 1):
            table_name = table['name']
            alias = table['alias']
            # Find matching join condition
            join_info = None
            for tables, info in join_conditions.items():
                if alias in tables:
                    join_info = info
                    break
            
            join_type = join_info['type'] if join_info else 'JOIN'
            join_condition = join_info['condition'] if join_info else ''
            
            create_sql += f"""
{join_type} (
    {subqueries[table_name]}
) {alias} ON {join_condition}"""

        # Add GROUP BY if present
        if analysis['grouping']:
            create_sql += f"\nGROUP BY {analysis['grouping']}"

        # Add refresh command
        refresh_sql = f"""
-- Refresh materialized view
REFRESH MATERIALIZED VIEW {view_name};
"""

        return f"""
-- Create Materialized View
{create_sql};

{refresh_sql}
"""

    def __del__(self):
        """Close all database connections."""
        for conn in self.connections.values():
            try:
                conn.close()
            except:
                pass

if __name__ == "__main__":
    # Initialize analyzer with config path
    analyzer = QueryAnalyzer('config.yaml')
    
    query = """
SELECT 
    steam.name AS steam_game_name,
    steam.price AS steam_price,
    epic.name AS epic_game_name,
    epic.price AS epic_price,
    epic.platform
FROM 
    steam_games steam
JOIN 
    epic_games epic
ON 
    steam.id = epic.game_id
WHERE 
    steam.price IS NOT NULL AND epic.price IS NOT NULL
ORDER BY 
    GREATEST(steam.price, epic.price) DESC
LIMIT 5;
    """

    analyzer.analyze_and_decompose_query(query)