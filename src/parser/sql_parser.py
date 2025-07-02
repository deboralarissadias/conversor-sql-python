"""
SQL Parser

Parses SQL queries using sqlparse and extracts components for conversion.
"""

import sqlparse
from sqlparse.sql import Statement, Token, TokenList, IdentifierList, Identifier
from sqlparse.tokens import Keyword, Name, Punctuation, Whitespace
from typing import Dict, List, Optional, Any
import re


class ParsedQuery:
    """Represents a parsed SQL query with its components."""
    
    def __init__(self):
        self.select_columns: List[str] = []
        self.from_tables: List[str] = []
        self.joins: List[Dict[str, str]] = []
        self.where_conditions: List[str] = []
        self.group_by_columns: List[str] = []
        self.having_conditions: List[str] = []
        self.order_by_columns: List[str] = []
        self.subqueries: List['ParsedQuery'] = []
        self.aliases: Dict[str, str] = {}
        self.window_functions: List[Dict[str, Any]] = []
        self.aggregate_functions: List[str] = []
        self.original_query: str = ""
        self.query_type: str = "SELECT"
        # For INSERT queries
        self.insert_table: str = ""
        self.insert_columns: List[str] = []
        self.insert_values: List[str] = []
        # For CREATE TABLE queries
        self.create_table: str = ""
        self.column_definitions: List[Dict[str, str]] = []
        # For UPDATE queries
        self.update_table: str = ""
        self.update_sets: List[str] = []
        # For DELETE queries
        self.delete_table: str = ""
        # For CREATE DATABASE queries
        self.create_database: str = ""
        # For DROP queries
        self.drop_type: str = ""
        self.drop_object: str = ""
        # Error handling
        self.error_message: str = ""


class SQLParser:
    """Main SQL parser class using sqlparse."""
    
    def __init__(self):
        self.aggregate_functions = {
            'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 
            'STDDEV', 'VARIANCE', 'MEDIAN'
        }
        self.window_functions = {
            'ROW_NUMBER', 'RANK', 'DENSE_RANK', 'NTILE',
            'LAG', 'LEAD', 'FIRST_VALUE', 'LAST_VALUE'
        }
    
    def parse(self, sql_query: str) -> ParsedQuery:
        """Parse SQL query and return ParsedQuery object."""
        parsed_query = ParsedQuery()
        parsed_query.original_query = sql_query.strip()
        
        # Clean and normalize the SQL
        sql_text = self._clean_sql(sql_query)
        
        # Determine query type
        if sql_text.upper().startswith('INSERT'):
            parsed_query.query_type = "INSERT"
            self._parse_insert_query(sql_text, parsed_query)
        elif sql_text.upper().startswith('CREATE TABLE'):
            parsed_query.query_type = "CREATE_TABLE"
            self._parse_create_table_query(sql_text, parsed_query)
        else:
            # Default to SELECT query parsing
            self._extract_select_columns(sql_text, parsed_query)
            self._extract_from_tables(sql_text, parsed_query)
            self._extract_where_conditions(sql_text, parsed_query)
            self._extract_joins(sql_text, parsed_query)
            self._extract_group_by(sql_text, parsed_query)
            self._extract_order_by(sql_text, parsed_query)
        
        return parsed_query
    
    def _parse_insert_query(self, sql_text: str, query: ParsedQuery):
        """Parse INSERT INTO query."""
        # Pattern to match INSERT INTO table_name (col1, col2, ...) VALUES (val1, val2, ...)
        insert_pattern = r'INSERT\s+INTO\s+(\w+)(?:\s*\((.*?)\))?\s+VALUES\s*\((.*?)\)'
        match = re.search(insert_pattern, sql_text, re.IGNORECASE | re.DOTALL)
        
        if match:
            query.insert_table = match.group(1).strip()
            
            # Extract columns if specified
            if match.group(2):
                columns_str = match.group(2).strip()
                query.insert_columns = [col.strip() for col in columns_str.split(',')]
            
            # Extract values
            values_str = match.group(3).strip()
            query.insert_values = self._split_values(values_str)
        else:
            # Try alternative pattern for INSERT INTO table_name VALUES (val1, val2, ...)
            alt_pattern = r'INSERT\s+INTO\s+(\w+)\s+VALUES\s*\((.*?)\)'
            match = re.search(alt_pattern, sql_text, re.IGNORECASE | re.DOTALL)
            
            if match:
                query.insert_table = match.group(1).strip()
                values_str = match.group(2).strip()
                query.insert_values = self._split_values(values_str)
    
    def _parse_create_table_query(self, sql_text: str, query: ParsedQuery):
        """Parse CREATE TABLE query."""
        # Pattern to match CREATE TABLE table_name (col1 type1, col2 type2, ...)
        create_pattern = r'CREATE\s+TABLE\s+(\w+)\s*\((.*?)\)'
        match = re.search(create_pattern, sql_text, re.IGNORECASE | re.DOTALL)
        
        if match:
            query.create_table = match.group(1).strip()
            columns_str = match.group(2).strip()
            
            # Split column definitions by comma, handling nested parentheses
            column_defs = self._split_columns(columns_str)
            
            for col_def in column_defs:
                col_def = col_def.strip()
                if col_def:
                    # Extract column name and type
                    parts = col_def.split(None, 1)
                    if len(parts) >= 2:
                        col_name = parts[0].strip()
                        col_type = parts[1].strip()
                        
                        # Check for constraints
                        constraints = []
                        if "NOT NULL" in col_type.upper():
                            constraints.append("NOT NULL")
                            col_type = col_type.replace("NOT NULL", "").strip()
                        if "PRIMARY KEY" in col_type.upper():
                            constraints.append("PRIMARY KEY")
                            col_type = col_type.replace("PRIMARY KEY", "").strip()
                        
                        query.column_definitions.append({
                            "name": col_name,
                            "type": col_type,
                            "constraints": constraints
                        })
    
    def _split_values(self, values_str: str) -> List[str]:
        """Split values by comma, handling nested quotes and parentheses."""
        return self._split_columns(values_str)  # Reuse the same logic
    
    def _clean_sql(self, sql_query: str) -> str:
        """Clean and normalize SQL query."""
        # Remove SQL comments (-- style)
        sql = re.sub(r'--.*$', '', sql_query, flags=re.MULTILINE)
        # Remove extra whitespace and normalize
        sql = re.sub(r'\s+', ' ', sql.strip())
        return sql
    
    def _extract_select_columns(self, sql_text: str, query: ParsedQuery):
        """Extract SELECT columns."""
        # Pattern to match SELECT ... FROM
        select_pattern = r'SELECT\s+(.*?)\s+FROM'
        match = re.search(select_pattern, sql_text, re.IGNORECASE | re.DOTALL)
        
        if match:
            select_part = match.group(1).strip()
            
            # Handle different column patterns
            if select_part.strip() == '*':
                query.select_columns = ['*']
            else:
                # Split by comma, handling nested parentheses
                columns = self._split_columns(select_part)
                for col in columns:
                    col = col.strip()
                    if col:
                        self._process_column(col, query)
        else:
            # Fallback for queries without FROM
            select_fallback = r'SELECT\s+(.*?)(?:WHERE|GROUP|ORDER|HAVING|$)'
            match = re.search(select_fallback, sql_text, re.IGNORECASE | re.DOTALL)
            
            if match:
                select_part = match.group(1).strip()
                if select_part.strip() == '*':
                    query.select_columns = ['*']
                else:
                    columns = self._split_columns(select_part)
                    for col in columns:
                        col = col.strip()
                        if col:
                            self._process_column(col, query)
    
    def _split_columns(self, select_part: str) -> List[str]:
        """Split columns by comma, handling nested parentheses and functions."""
        columns = []
        current_col = ""
        paren_depth = 0
        in_quotes = False
        quote_char = None
        
        for char in select_part:
            if char in ('"', "'") and not in_quotes:
                in_quotes = True
                quote_char = char
            elif char == quote_char and in_quotes:
                in_quotes = False
                quote_char = None
            elif not in_quotes:
                if char == '(':
                    paren_depth += 1
                elif char == ')':
                    paren_depth -= 1
                elif char == ',' and paren_depth == 0:
                    columns.append(current_col.strip())
                    current_col = ""
                    continue
            
            current_col += char
        
        if current_col.strip():
            columns.append(current_col.strip())
        
        return columns
    
    def _process_column(self, col: str, query: ParsedQuery):
        """Process individual column and extract information."""
        col = col.strip()
        
        # Check for aliases (AS keyword or space-separated)
        alias_match = re.search(r'\s+AS\s+(\w+)$', col, re.IGNORECASE)
        if alias_match:
            alias = alias_match.group(1)
            col_name = col[:alias_match.start()].strip()
            query.aliases[alias] = col_name
            query.select_columns.append(f"{col_name} AS {alias}")
        else:
            # Check for space-separated alias (column alias_name)
            parts = col.split()
            if len(parts) >= 2 and not any(func in col.upper() for func in self.aggregate_functions):
                # Likely has an alias
                alias = parts[-1]
                col_name = ' '.join(parts[:-1])
                query.aliases[alias] = col_name
                query.select_columns.append(f"{col_name} AS {alias}")
            else:
                query.select_columns.append(col)
        
        # Check for aggregate functions
        for func in self.aggregate_functions:
            if func in col.upper():
                query.aggregate_functions.append(col)
                break
    
    def _extract_from_tables(self, sql_text: str, query: ParsedQuery):
        """Extract FROM tables."""
        # Pattern to match FROM ... (WHERE|JOIN|GROUP|ORDER|HAVING|$)
        from_pattern = r'FROM\s+(.*?)(?:\s+(?:INNER\s+JOIN|LEFT\s+JOIN|RIGHT\s+JOIN|FULL\s+JOIN|JOIN|WHERE|GROUP|ORDER|HAVING)|$)'
        match = re.search(from_pattern, sql_text, re.IGNORECASE)
        
        if match:
            from_part = match.group(1).strip()
            
            # Handle table with alias
            alias_match = re.search(r'(\w+)\s+(\w+)$', from_part)
            if alias_match:
                table_name = alias_match.group(1)
                alias = alias_match.group(2)
                query.aliases[alias] = table_name
                query.from_tables.append(f"{table_name} AS {alias}")
            else:
                query.from_tables.append(from_part)
    
    def _extract_where_conditions(self, sql_text: str, query: ParsedQuery):
        """Extract WHERE conditions."""
        # Pattern to match WHERE ... (GROUP|ORDER|HAVING|$)
        where_pattern = r'WHERE\s+(.*?)(?:\s+(?:GROUP|ORDER|HAVING)|$)'
        match = re.search(where_pattern, sql_text, re.IGNORECASE | re.DOTALL)
        
        if match:
            where_part = match.group(1).strip()
            
            # For simple cases, just add the whole WHERE clause as one condition
            # This preserves the original logic and operators
            query.where_conditions.append(where_part)
    
    def _extract_joins(self, sql_text: str, query: ParsedQuery):
        """Extract JOIN clauses."""
        # Pattern to match different types of JOINs
        join_pattern = r'((?:INNER\s+|LEFT\s+|RIGHT\s+|FULL\s+)?JOIN)\s+(\S+)(?:\s+(?:AS\s+)?(\w+))?\s+ON\s+([^JOIN]+?)(?=\s+(?:JOIN|WHERE|GROUP|ORDER|HAVING|$))'
        
        matches = re.finditer(join_pattern, sql_text, re.IGNORECASE | re.DOTALL)
        
        for match in matches:
            join_type = match.group(1).strip()
            table_name = match.group(2).strip()
            table_alias = match.group(3) if match.group(3) else None
            join_condition = match.group(4).strip()
            
            join_info = {
                'type': join_type,
                'table': table_name,
                'condition': join_condition
            }
            
            if table_alias:
                join_info['alias'] = table_alias
                query.aliases[table_alias] = table_name
            
            query.joins.append(join_info)
    
    def _extract_group_by(self, sql_text: str, query: ParsedQuery):
        """Extract GROUP BY columns."""
        # Pattern to match GROUP BY ... (ORDER|HAVING|$)
        group_by_pattern = r'GROUP\s+BY\s+(.*?)(?:\s+(?:ORDER|HAVING|$))'
        match = re.search(group_by_pattern, sql_text, re.IGNORECASE | re.DOTALL)
        
        if match:
            group_by_part = match.group(1).strip()
            
            # Split by comma
            columns = [col.strip() for col in group_by_part.split(',')]
            query.group_by_columns = [col for col in columns if col]
    
    def _extract_order_by(self, sql_text: str, query: ParsedQuery):
        """Extract ORDER BY columns."""
        # Pattern to match ORDER BY ... ($)
        order_by_pattern = r'ORDER\s+BY\s+(.*?)(?:\s*$)'
        match = re.search(order_by_pattern, sql_text, re.IGNORECASE | re.DOTALL)
        
        if match:
            order_by_part = match.group(1).strip()
            
            # Split by comma and handle ASC/DESC
            columns = [col.strip() for col in order_by_part.split(',')]
            query.order_by_columns = [col for col in columns if col]
