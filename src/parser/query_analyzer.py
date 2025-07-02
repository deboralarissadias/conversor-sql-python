"""
Query Analyzer

Analyzes parsed SQL queries and extracts additional information.
"""

from typing import Dict, List, Any
from .sql_parser import ParsedQuery


class QueryAnalyzer:
    """Analyzes parsed SQL queries to extract additional information."""
    
    def __init__(self):
        self.supported_features = {
            'joins': True,
            'subqueries': True,
            'window_functions': True,
            'aggregations': True,
            'case_statements': True
        }
    
    def analyze(self, parsed_query: ParsedQuery) -> Dict[str, Any]:
        """Analyze a parsed query and return additional information."""
        analysis = {
            'query_type': parsed_query.query_type,
            'tables_referenced': self._get_referenced_tables(parsed_query),
            'columns_referenced': self._get_referenced_columns(parsed_query),
            'has_joins': len(parsed_query.joins) > 0,
            'has_aggregations': len(parsed_query.aggregate_functions) > 0,
            'has_window_functions': len(parsed_query.window_functions) > 0,
            'complexity': self._calculate_complexity(parsed_query)
        }
        
        return analysis
    
    def _get_referenced_tables(self, query: ParsedQuery) -> List[str]:
        """Extract all referenced tables from the query."""
        tables = []
        
        # Add main tables from FROM clause
        tables.extend(query.from_tables)
        
        # Add tables from JOINs
        for join in query.joins:
            if 'table' in join:
                tables.append(join['table'])
        
        # Remove aliases and duplicates
        clean_tables = []
        for table in tables:
            # Remove "AS alias" if present
            if " AS " in table.upper():
                table = table.split(" AS ")[0].strip()
            
            # Add if not already in the list
            if table not in clean_tables:
                clean_tables.append(table)
        
        return clean_tables
    
    def _get_referenced_columns(self, query: ParsedQuery) -> List[str]:
        """Extract all referenced columns from the query."""
        columns = []
        
        # Add columns from SELECT
        for col in query.select_columns:
            if col != '*':
                # Extract column name (remove aliases and functions)
                # This is a simplified approach
                col_name = col.split(" AS ")[0].strip() if " AS " in col else col
                columns.append(col_name)
        
        # Add columns from WHERE conditions
        for condition in query.where_conditions:
            # This is a simplified approach - in a real implementation,
            # we would parse the condition to extract column names
            columns.append(condition)
        
        # Add columns from GROUP BY
        columns.extend(query.group_by_columns)
        
        # Add columns from ORDER BY
        columns.extend(query.order_by_columns)
        
        return columns
    
    def _calculate_complexity(self, query: ParsedQuery) -> str:
        """Calculate query complexity based on various factors."""
        complexity_score = 0
        
        # Add points for each feature
        complexity_score += len(query.from_tables)
        complexity_score += len(query.joins) * 2
        complexity_score += len(query.where_conditions)
        complexity_score += len(query.group_by_columns)
        complexity_score += len(query.order_by_columns)
        complexity_score += len(query.aggregate_functions)
        complexity_score += len(query.window_functions) * 2
        complexity_score += len(query.subqueries) * 3
        
        # Determine complexity level
        if complexity_score <= 5:
            return "Simple"
        elif complexity_score <= 15:
            return "Moderate"
        else:
            return "Complex"