from qa import QueryInsights
import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Token
from sqlparse.tokens import Keyword, DML

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
            return None
        
        full_name = identifier.value.strip()
        alias = None
        table_alias = None
        column_name = full_name
        
        # Handle AS alias
        if ' AS ' in full_name.upper():
            parts = full_name.split(' AS ')
            column_name = parts[0].strip()
            alias = parts[1].strip()
        
        # Handle table qualification for non-aggregate functions
        if not any(agg in column_name.upper() for agg in ('SUM(', 'COUNT(', 'AVG(', 'MAX(', 'MIN(')):
            if '.' in column_name:
                parts = column_name.split('.')
                table_alias = parts[0].strip()
                column_name = parts[1].strip()
        else:
            # For aggregate functions, extract table alias if present
            if '.' in column_name:
                # Extract table alias from within the function
                # e.g., SUM(ps.price) -> table_alias = ps
                match = column_name.split('(')[1].split('.')[0].strip()
                if match:
                    table_alias = match
        
        return {
            'table_alias': table_alias,
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
        
        # Get the actual column names from config for case sensitivity
        table_schema = self._get_table_schema(table_alias)
        
        # Handle both regular and aggregate columns from SELECT
        for col in columns:
            if not col['table_alias'] or col['table_alias'] == table_alias:
                # Handle aggregate functions (SUM, COUNT, AVG, etc.)
                if any(agg in col['full_reference'].upper() for agg in ('SUM(', 'COUNT(', 'AVG(', 'MAX(', 'MIN(')):
                    modified_reference = col['full_reference']
                    if '.' in modified_reference:
                        agg_start = modified_reference.find('(')
                        agg_end = modified_reference.find(')')
                        if agg_start != -1 and agg_end != -1:
                            inner_content = modified_reference[agg_start+1:agg_end]
                            if '.' in inner_content:
                                _, column_part = inner_content.split('.')
                                # Get correct case from schema
                                if table_schema:
                                    column_part = self._get_correct_case(column_part.strip(), table_schema)
                                modified_reference = (
                                    modified_reference[:agg_start+1] + 
                                    column_part + 
                                    modified_reference[agg_end:]
                                )
                    table_columns.add(modified_reference)
                elif col['table_alias'] == table_alias:
                    # For regular columns, preserve case from schema
                    column = col['column']
                    if table_schema:
                        column = self._get_correct_case(column, table_schema)
                    if col['alias']:
                        column = f"{column} AS {col['alias']}"
                    table_columns.add(column)
        
        # Add join columns with correct case
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
                            join_column = join_column.strip('"')
                            if table_schema:
                                join_column = self._get_correct_case(join_column, table_schema)
                            table_columns.add(join_column)
        
        return list(table_columns)
    
    def _get_table_schema(self, table_alias):
        """Get the table schema from config using table alias."""
        if not self.db_config:
            return None
        
        for table in self.analysis['tables_involved']:
            if table['alias'] == table_alias or table['name'] == table_alias:
                table_name = table['name']
                for db in self.db_config.get('databases', []):
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
