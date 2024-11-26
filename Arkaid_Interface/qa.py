from sqlanalyzer import query_analyzer
import re

class QueryInsights:
    def __init__(self, query, db_config):
        self.query = query.strip()
        self.analyzer = query_analyzer.Analyzer(query)
        self.db_config = db_config
        
    def _get_table_columns(self, table_name):
        """Get columns for a specific table from config."""
        if not self.db_config:
            return []
            
        for db in self.db_config.get('databases', []):
            for table in db.get('tables', []):
                if table['name'] == table_name:
                    return list(table['columns'].keys())
        return []
    
    def get_detailed_analysis(self):
        try:
            basic_analysis = self.analyzer.parse_query(self.query)
            
            # Enhanced analysis
            analysis_result = {
                'query_type': self._get_query_type(),
                'tables_involved': self._extract_tables(),
                'selected_columns': self._extract_columns(),
                'aggregations': self._find_aggregations(),
                'joins': self._analyze_joins(),
                'grouping': self._analyze_grouping(),
                'having': self._analyze_having(),
                'ordering': self._analyze_ordering(),
                'performance_tips': self._generate_performance_tips()
            }
            
            return analysis_result
        except Exception as e:
            return {
                'error': f"Failed to analyze query: {str(e)}",
                'query': self.query
            }
    
    def _get_query_type(self):
        query_start = self.query.strip().upper().split()[0]
        operation_types = {
            'SELECT': 'SELECT (Read Operation)',
            'INSERT': 'INSERT (Create Operation)',
            'UPDATE': 'UPDATE (Update Operation)',
            'DELETE': 'DELETE (Delete Operation)',
            'MERGE': 'MERGE (Upsert Operation)'
        }
        return operation_types.get(query_start, 'Unknown Operation')
    
    def _extract_tables(self):
        try:
            # Use regex to find table names from FROM and JOIN clauses
            query_upper = self.query.upper()
            # Match table names including their aliases
            table_pattern = r'(?:FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\s+(?:AS\s+)?[a-zA-Z0-9_]+)?)'
            matches = re.finditer(table_pattern, query_upper)
            
            tables = []
            for match in matches:
                table_ref = match.group(1).lower()
                # Split to handle aliases
                table_parts = table_ref.split()
                tables.append({
                    'name': table_parts[0],
                    'alias': table_parts[-1] if len(table_parts) > 1 else None
                })
            return tables
        except Exception:
            return []
    
    def _extract_columns(self):
        try:
            # Extract everything between SELECT and FROM
            select_pattern = r'SELECT\s+(.*?)\s+FROM'
            match = re.search(select_pattern, self.query, re.IGNORECASE | re.DOTALL)
            if not match:
                return []
            
            select_part = match.group(1)
            columns = []
            
            # Split by comma but respect parentheses
            current_column = ''
            paren_count = 0
            
            for char in select_part:
                if char == '(':
                    paren_count += 1
                elif char == ')':
                    paren_count -= 1
                elif char == ',' and paren_count == 0:
                    if current_column.strip():
                        columns.append(current_column.strip())
                    current_column = ''
                    continue
                current_column += char
            
            if current_column.strip():
                columns.append(current_column.strip())
                
            return columns
        except Exception:
            return []
    
    def _find_aggregations(self):
        try:
            select_part = self.query.split('FROM')[0].replace('SELECT', '').strip()
            
            agg_functions = ['AVG', 'MAX', 'MIN', 'COUNT', 'SUM', 'STDDEV', 'VARIANCE']
            found_aggs = []
            
            for agg in agg_functions:
                pattern = rf'\b{agg}\s*\(' # Match function name followed by parenthesis
                if re.search(pattern, select_part.upper()):
                    found_aggs.append(agg)
            
            return found_aggs
        except Exception:
            return []
    
    def _analyze_joins(self):
        try:
            join_types = ['INNER JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'FULL JOIN', 'CROSS JOIN', 'JOIN']
            joins = []
            
            query_upper = self.query.upper()
            for join_type in join_types:
                join_positions = [m.start() for m in re.finditer(rf'\b{join_type}\b', query_upper)]
                
                for pos in join_positions:
                    # Find the ON clause for this join
                    join_part = query_upper[pos:]
                    on_match = re.search(r'\bON\b(.*?)(?:\b(?:JOIN|WHERE|GROUP BY|ORDER BY)\b|$)', join_part, re.DOTALL)
                    
                    if on_match:
                        condition = on_match.group(1).strip()
                        joins.append({
                            'type': join_type,
                            'condition': condition
                        })
            
            return joins if joins else None
        except Exception:
            return None
    
    def _analyze_grouping(self):
        try:
            group_pattern = r'GROUP BY\s+(.*?)(?:\s+(?:HAVING|ORDER BY)|;|\s*$)'
            match = re.search(group_pattern, self.query, re.IGNORECASE | re.DOTALL)
            if match:
                return match.group(1).strip()
            return None
        except Exception:
            return None
    
    def _analyze_having(self):
        try:
            having_pattern = r'HAVING\s+(.*?)(?:\s+ORDER BY|;|\s*$)'
            match = re.search(having_pattern, self.query, re.IGNORECASE | re.DOTALL)
            if match:
                return match.group(1).strip()
            return None
        except Exception:
            return None
    
    def _analyze_ordering(self):
        try:
            order_pattern = r'ORDER BY\s+(.*?)(?:;|\s*$)'
            match = re.search(order_pattern, self.query, re.IGNORECASE | re.DOTALL)
            if match:
                order_part = match.group(1).strip()
                orders = []
                
                for item in order_part.split(','):
                    item = item.strip()
                    if ' DESC' in item.upper():
                        direction = 'DESC'
                        column = item.upper().replace(' DESC', '').strip()
                    elif ' ASC' in item.upper():
                        direction = 'ASC'
                        column = item.upper().replace(' ASC', '').strip()
                    else:
                        direction = 'ASC'  # Default
                        column = item
                    
                    orders.append({
                        'column': column,
                        'direction': direction
                    })
                
                return orders
            return None
        except Exception:
            return None
    
    def _generate_performance_tips(self):
        tips = []
        try:
            tables = self._extract_tables()
            if len(tables) > 1:
                tips.append("Consider indexing the JOIN columns for better performance")
            
            if 'GROUP BY' in self.query.upper():
                tips.append("Ensure proper indexing on GROUP BY columns")
            
            if self._analyze_having():
                tips.append("HAVING clause detected. Ensure efficient filtering of grouped results")
            
            if self._analyze_ordering():
                tips.append("Consider indexing ORDER BY columns")
            
            if len(tables) > 3:
                tips.append("Large number of joins detected. Consider reviewing query structure")
            
            aggs = self._find_aggregations()
            if len(aggs) > 0:
                tips.append("Aggregations detected. Ensure proper indexing for grouped columns")
            
            return tips
        except Exception:
            return ["Unable to generate performance tips"]