from qa import QueryInsights
import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Token
from sqlparse.tokens import Keyword, DML
import re

class QueryDecomposer:
    def __init__(self, query, db_config):
        self.query = query
        self.db_config = db_config
        self.insights = QueryInsights(query, db_config)
        self.analysis = self.insights.get_detailed_analysis()
        self.parsed = sqlparse.parse(query)[0]
    
    def _validate_table_columns(self, table_name, columns):
        """Validate that columns exist in the table schema."""
        if not self.db_config:
            return columns
            
        valid_columns = []
        table_schema = None
        
        # Find table schema
        for db in self.db_config.get('databases', []):
            for table in db.get('tables', []):
                if table['name'] == table_name:
                    table_schema = table['columns']
                    break
            if table_schema:
                break
                
        if not table_schema:
            return columns
            
        # Validate columns
        for col in columns:
            # Handle aggregations
            if any(agg in col.upper() for agg in ('COUNT', 'SUM', 'AVG', 'MAX', 'MIN')):
                valid_columns.append(col)
                continue
                
            # Remove alias if present
            if ' AS ' in col:
                col = col.split(' AS ')[0].strip()
                
            # Remove table qualification
            if '.' in col:
                col = col.split('.')[1].strip()
                
            if col in table_schema:
                valid_columns.append(col)
                
        return valid_columns
    
    def generate_table_subqueries(self):
        """Generate individual subqueries for each table in the main query."""
        tables = self.analysis['tables_involved']
        if not tables:
            return {'error': 'No tables found in query'}
        
        # If no JOINs, return the original query for the single table
        if not self.analysis['joins']:
            return {tables[0]['name']: self.query}
        
        subqueries = {}
        selected_columns = self._parse_selected_columns()
        
        for table in tables:
            table_name = table['name']
            table_alias = table['alias'] or table_name
            
            # Get columns specific to this table
            table_columns = self._get_table_specific_columns(table_alias, selected_columns)
            
            # Create subquery
            subquery = self._create_subquery(table_name, table_alias, table_columns)
            if subquery:  # Only add if subquery is not None
                subqueries[table_name] = subquery
            
        return subqueries
    
    def _parse_selected_columns(self):
        """Parse the selected columns using sqlparse."""
        parsed_columns = []
        
        # Find the SELECT statement
        for token in self.parsed.tokens:
            if token.ttype is DML and token.value.upper() == 'SELECT':
                # Get the next token containing the columns
                columns_token = self._get_next_token(token)
                if isinstance(columns_token, IdentifierList):
                    identifiers = columns_token.get_identifiers()
                else:
                    identifiers = [columns_token]
                
                for identifier in identifiers:
                    col = self._parse_column_identifier(identifier)
                    if col:
                        parsed_columns.append(col)
                break
        
        return parsed_columns
    
    def _parse_column_identifier(self, identifier):
        """Parse a column identifier into its components."""
        if not identifier:
            return {
                'table_alias': '',
                'column': '*',
                'alias': None,
                'full_reference': '*'
            }
        
        full_name = identifier.value.strip()
        alias = None
        table_alias = None
        column_name = full_name
        
        # Handle COUNT(*) and other aggregates with *
        agg_funcs = ['COUNT', 'SUM', 'AVG', 'MAX', 'MIN']
        for func in agg_funcs:
            if f'{func}(*' in full_name.upper():
                # Check if there's an alias
                parts = full_name.split(' AS ' if ' AS ' in full_name else ' as ')
                if len(parts) > 1:
                    alias = parts[1].strip()
                return {
                    'table_alias': '',
                    'column': parts[0].strip(),
                    'alias': alias,
                    'full_reference': full_name
                }
        
        # Handle AS alias
        if ' AS ' in full_name.upper():
            parts = full_name.split(' AS ' if ' AS ' in full_name else ' as ')
            column_name = parts[0].strip()
            alias = parts[1].strip() if len(parts) > 1 else None
        
        # Handle table qualification
        if '.' in column_name:
            parts = column_name.split('.')
            table_alias = parts[0].strip()
            column_name = parts[1].strip()
        
        # Handle aggregate functions
        for func in agg_funcs:
            if f'{func}(' in column_name.upper():
                # Extract table alias if present in the function argument
                func_args = column_name[column_name.find('(')+1:column_name.rfind(')')]
                if '.' in func_args:
                    table_alias = func_args.split('.')[0].strip()
        
        return {
            'table_alias': table_alias or '',
            'column': column_name,
            'alias': alias,
            'full_reference': full_name
        }
    
    def _get_next_token(self, token):
        """Get the next meaningful token after the given token."""
        found_current = False
        for t in self.parsed.tokens:
            if found_current and not t.is_whitespace:
                return t
            if t == token:
                found_current = True
        return None
    
    def _get_table_specific_columns(self, table_alias, columns):
        """Get columns specific to a given table alias."""
        table_columns = set()
        base_columns = set()
        
        # Handle SELECT *
        if len(columns) == 1 and columns[0]['column'] == '*':
            # Get all columns from table schema
            table_schema = self._get_table_schema(table_alias)
            if table_schema:
                return list(table_schema)
            return ['*']  # Fallback if schema not available
        
        # Get the actual column names from config for case sensitivity
        table_schema = self._get_table_schema(table_alias)
        
        # First, add join columns to ensure they're included without aliases
        joins = self.analysis['joins']
        if joins:
            for join in joins:
                condition = join['condition']
                parts = condition.split('=')
                for part in parts:
                    part = part.strip()
                    if '.' in part:
                        join_table_alias, join_column = part.split('.')
                        join_table_alias = join_table_alias.strip()
                        if join_table_alias.upper() == table_alias.upper():
                            join_column = join_column.strip('"').lower()
                            if table_schema:
                                join_column = self._get_correct_case(join_column, table_schema)
                            if join_column.lower() not in base_columns:
                                table_columns.add(join_column)
                                base_columns.add(join_column.lower())
        
        # Add columns used in GROUP BY
        if 'GROUP BY' in self.query.upper():
            group_clause = re.search(r'GROUP\s+BY\s+([^;]+?)(?:\s+(?:HAVING|ORDER|LIMIT)|;|$)', 
                                   self.query, re.IGNORECASE | re.DOTALL)
            if group_clause:
                group_cols = group_clause.group(1).split(',')
                for col in group_cols:
                    col = col.strip()
                    if '.' in col:
                        col_alias, col_name = col.split('.')
                        if col_alias.strip().upper() == table_alias.upper():
                            col_name = col_name.strip().lower()
                            if table_schema:
                                col_name = self._get_correct_case(col_name, table_schema)
                            if col_name.lower() not in base_columns:
                                table_columns.add(col_name)
                                base_columns.add(col_name.lower())
        
        # Add columns used in WHERE clause
        if 'WHERE' in self.query.upper():
            where_clause = self.query.upper().split('WHERE')[1]
            for keyword in ['GROUP BY', 'ORDER BY', 'HAVING', 'LIMIT']:
                if keyword in where_clause:
                    where_clause = where_clause.split(keyword)[0]
            where_clause = where_clause.split(';')[0].strip()
            
            where_parts = re.findall(r'([A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*)', where_clause)
            for part in where_parts:
                where_table_alias, where_column = part.split('.')
                where_table_alias = where_table_alias.strip()
                if where_table_alias.upper() == table_alias.upper():
                    where_column = where_column.strip().lower()
                    if table_schema:
                        where_column = self._get_correct_case(where_column, table_schema)
                    if where_column.lower() not in base_columns:
                        table_columns.add(where_column)
                        base_columns.add(where_column.lower())
        
        # Then handle selected columns
        for col in columns:
            col_table_alias = col.get('table_alias', '').upper()
            if not col_table_alias or col_table_alias == table_alias.upper():
                column = col['column']
                
                # Handle aggregate functions
                if any(agg in column.upper() for agg in ('COUNT(', 'SUM(', 'AVG(', 'MAX(', 'MIN(')):
                    # For COUNT(*), we don't need to add any columns
                    if column.upper() == 'COUNT(*)':
                        continue
                    # For other aggregates, extract the column name
                    match = re.search(r'\((.*?)\)', column)
                    if match:
                        agg_col = match.group(1).strip()
                        if '.' in agg_col:
                            _, agg_col = agg_col.split('.')
                        agg_col = agg_col.lower()
                        if table_schema:
                            agg_col = self._get_correct_case(agg_col, table_schema)
                        if agg_col.lower() not in base_columns:
                            table_columns.add(agg_col)
                            base_columns.add(agg_col.lower())
                else:
                    # For regular columns
                    if '.' in column:
                        _, column = column.split('.')
                    column = column.lower()
                    if table_schema:
                        column = self._get_correct_case(column, table_schema)
                    
                    # If this column is used in a join, don't alias it
                    if column.lower() in base_columns:
                        table_columns.add(column)
                    else:
                        # Add alias if present
                        alias = col.get('alias')
                        if alias:
                            column = f"{column} AS {alias}"
                        table_columns.add(column)
                        base_columns.add(column.lower())
        
        return list(table_columns)
    
    def _get_table_schema(self, table_alias):
        """Get the table schema from config using table alias."""
        if not self.db_config:
            return None
        
        for table in self.analysis['tables_involved']:
            if table['alias'] == table_alias or table['name'] == table_alias:
                table_name = table['name']
                # Check if it's a materialized view (starts with mv_ or mv)
                is_mv = table_name.lower().startswith(('mv_', 'mv'))
                
                for db in self.db_config.get('databases', []):
                    # For materialized views, only look in DB3
                    if is_mv and db['name'] != 'DB3':
                        continue
                    
                    for table_config in db.get('tables', []):
                        if table_config['name'] == table_name:
                            return table_config['columns']
        return None
    
    def _get_correct_case(self, column_name, schema):
        """Get the correct case for a column name from schema."""
        # First try exact match
        if column_name in schema:
            return column_name
        
        # Try case-insensitive match
        for schema_column in schema:
            if schema_column.lower() == column_name.lower():
                return schema_column
                
        # If not found, return original
        return column_name
    
    def _create_subquery(self, table_name, table_alias, columns):
        """Create a subquery for a specific table."""
        if not columns:
            return None
        
        # Build SELECT clause
        select_clause = "SELECT " + ", ".join(columns)
        from_clause = f"FROM {table_name}"
        
        # Add GROUP BY if we have aggregations
        group_by = ""
        non_agg_columns = []
        
        for col in columns:
            # Skip aggregate functions and aliases when adding to GROUP BY
            if not any(agg in col.upper() for agg in ('SUM(', 'COUNT(', 'AVG(', 'MAX(', 'MIN(')):
                # Remove alias if present
                clean_col = col.split(' AS ')[0] if ' AS ' in col else col
                non_agg_columns.append(clean_col)
        
        if any(agg in col.upper() for col in columns for agg in ('SUM(', 'COUNT(', 'AVG(', 'MAX(', 'MIN(')):
            if non_agg_columns:
                group_by = "\nGROUP BY " + ", ".join(non_agg_columns)
        
        return f"{select_clause}\n{from_clause}{group_by}"
    
    def get_join_conditions(self):
        """Extract join conditions between tables using sqlparse."""
        joins = self.analysis['joins']
        if not joins:
            return {}
        
        join_conditions = {}
        for join in joins:
            condition = join['condition']
            tables_involved = self._extract_tables_from_condition(condition)
            join_conditions[tuple(sorted(tables_involved))] = {
                'type': join['type'],
                'condition': condition
            }
            
        return join_conditions
    
    def _extract_tables_from_condition(self, condition):
        """Extract table aliases from a join condition using sqlparse."""
        parsed = sqlparse.parse(condition)[0]
        tables = set()
        
        for token in parsed.flatten():
            if isinstance(token, Identifier) and '.' in token.value:
                table = token.value.split('.')[0].strip()
                # Preserve original case
                tables.add(table)
                
        return list(tables)
    
    def convert_sql_where_to_pandas(self, where_clause):
        """Convert SQL WHERE clause to pandas query syntax."""
        # Remove table aliases from the where clause
        for table in self.analysis['tables_involved']:
            alias = table['alias'] or table['name']
            where_clause = where_clause.replace(f"{alias}.", "")
        
        # Convert column names to lowercase to match DataFrame columns
        where_clause = where_clause.lower()
        
        # Handle NULL conditions
        where_clause = where_clause.replace(' is null', '.isnull()')
        where_clause = where_clause.replace(' is not null', '.notnull()')
        
        # Basic conversions
        where_clause = where_clause.replace(' and ', ' & ')
        where_clause = where_clause.replace(' or ', ' | ')
        where_clause = where_clause.replace('=', '==')
        where_clause = where_clause.replace('<>', '!=')
        
        # Handle string literals
        where_clause = re.sub(r"'([^']*)'", r'"\1"', where_clause)
        
        return where_clause
