from qa import QueryInsights
import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Token
from sqlparse.tokens import Keyword, DML

class QueryDecomposer:
    def __init__(self, query):
        self.query = query
        self.insights = QueryInsights(query)
        self.analysis = self.insights.get_detailed_analysis()
        self.parsed = sqlparse.parse(query)[0]
    
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
            
        full_name = identifier.value
        alias = None
        table_alias = None
        column_name = full_name
        
        # Handle AS alias
        if ' AS ' in full_name.upper():
            parts = full_name.split(' AS ')
            column_name = parts[0].strip()
            alias = parts[1].strip()
        
        # Handle table qualification
        if '.' in column_name:
            parts = column_name.split('.')
            table_alias = parts[0].strip()
            column_name = parts[1].strip()
        
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
        table_columns = []
        
        for col in columns:
            # For regular columns (non-aggregates)
            if col['table_alias'] == table_alias:
                column = col['column']
                if col['alias']:
                    column = f"{column} AS {col['alias']}"
                table_columns.append(column)
            
            # For aggregated columns
            elif any(agg in col['full_reference'].upper() for agg in ('COUNT', 'SUM', 'AVG', 'MAX', 'MIN')):
                # Extract the table alias from the aggregation
                agg_content = col['full_reference']
                for agg in ('COUNT', 'SUM', 'AVG', 'MAX', 'MIN'):
                    if agg in agg_content.upper():
                        # Find content inside parentheses
                        start = agg_content.find('(') + 1
                        end = agg_content.find(')')
                        if start > 0 and end > start:
                            inner_content = agg_content[start:end].strip()
                            if '.' in inner_content:
                                inner_table_alias = inner_content.split('.')[0].strip()
                                if inner_table_alias == table_alias:
                                    # Remove table alias from the aggregation
                                    inner_column = inner_content.split('.')[1].strip()
                                    agg_col = f"{agg}({inner_column})"
                                    if col['alias']:
                                        agg_col += f" AS {col['alias']}"
                                    table_columns.append(agg_col)
                
        return table_columns
    
    def _create_subquery(self, table_name, table_alias, columns):
        """Create a subquery for a specific table."""
        if not columns:
            # Check if this table is only used in joins but has no selected columns
            # Return None or empty string to indicate no subquery needed
            return None
        
        # Handle aggregations
        has_aggregation = any(
            col.upper().startswith(('COUNT', 'SUM', 'AVG', 'MAX', 'MIN')) 
            for col in columns
        )
        
        # Clean columns - remove other table aliases
        cleaned_columns = []
        for col in columns:
            if any(agg in col.upper() for agg in ('COUNT', 'SUM', 'AVG', 'MAX', 'MIN')):
                # Keep aggregations but clean table references
                cleaned_columns.append(col)
            else:
                # For regular columns, remove any table qualification
                cleaned_columns.append(col.split('.')[-1] if '.' in col else col)
        
        # Build the SELECT clause
        select_clause = "SELECT " + ", ".join(cleaned_columns)
        
        # Build the FROM clause
        from_clause = f"FROM {table_name}"
        if table_alias != table_name:
            from_clause += f" AS {table_alias}"
            
        # Add GROUP BY if there are aggregations
        group_by = ""
        if has_aggregation:
            non_agg_columns = [
                col for col in cleaned_columns 
                if not any(agg in col.upper() for agg in ('COUNT', 'SUM', 'AVG', 'MAX', 'MIN'))
            ]
            if non_agg_columns:
                group_by = "\nGROUP BY " + ", ".join(non_agg_columns)
        
        # Combine all parts
        subquery = f"{select_clause}\n{from_clause}{group_by}"
        
        return subquery

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
                tables.add(table)
                
        return list(tables)
