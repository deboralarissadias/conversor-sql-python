"""
Multi-Query SQL Parser

Enhanced parser that can handle multiple SQL queries and more complex query types.
"""

import sqlparse
import re
from typing import List, Dict, Optional, Any
from .sql_parser import ParsedQuery, SQLParser


class MultiQueryParser(SQLParser):
    """Enhanced SQL parser that can handle multiple queries and more complex SQL."""
    
    def __init__(self):
        super().__init__()
        
    def parse_multiple(self, sql_text: str) -> List[ParsedQuery]:
        """Parse multiple SQL queries from a single string."""
        # Split queries using sqlparse
        statements = sqlparse.split(sql_text)
        
        parsed_queries = []
        for statement in statements:
            statement = statement.strip()
            if statement and not statement.startswith('--') and statement != ';':
                try:
                    parsed_query = self.parse(statement)
                    if parsed_query:
                        parsed_queries.append(parsed_query)
                except Exception as e:
                    # Create error query object
                    error_query = ParsedQuery()
                    error_query.original_query = statement
                    error_query.query_type = "ERROR"
                    error_query.error_message = str(e)
                    parsed_queries.append(error_query)
        
        return parsed_queries
    
    def parse(self, sql_query: str) -> ParsedQuery:
        """Enhanced parse method with better query type detection."""
        parsed_query = ParsedQuery()
        parsed_query.original_query = sql_query.strip()
        
        # Clean and normalize the SQL
        sql_text = self._clean_sql(sql_query)
        
        # Enhanced query type detection
        query_type = self._detect_query_type(sql_text)
        parsed_query.query_type = query_type
        
        try:
            if query_type == "SELECT":
                self._parse_select_query(sql_text, parsed_query)
            elif query_type == "INSERT":
                self._parse_insert_query(sql_text, parsed_query)
            elif query_type == "UPDATE":
                self._parse_update_query(sql_text, parsed_query)
            elif query_type == "DELETE":
                self._parse_delete_query(sql_text, parsed_query)
            elif query_type == "CREATE_TABLE":
                self._parse_create_table_query(sql_text, parsed_query)
            elif query_type == "CREATE_DATABASE":
                self._parse_create_database_query(sql_text, parsed_query)
            elif query_type == "DROP":
                self._parse_drop_query(sql_text, parsed_query)
            else:
                # Fallback to basic SELECT parsing
                self._parse_select_query(sql_text, parsed_query)
                
        except Exception as e:
            parsed_query.error_message = str(e)
            
        return parsed_query
    
    def _detect_query_type(self, sql_text: str) -> str:
        """Enhanced query type detection."""
        sql_upper = sql_text.upper().strip()
        
        if sql_upper.startswith('SELECT'):
            return "SELECT"
        elif sql_upper.startswith('INSERT'):
            return "INSERT"
        elif sql_upper.startswith('UPDATE'):
            return "UPDATE"
        elif sql_upper.startswith('DELETE'):
            return "DELETE"
        elif sql_upper.startswith('CREATE TABLE'):
            return "CREATE_TABLE"
        elif sql_upper.startswith('CREATE DATABASE'):
            return "CREATE_DATABASE"
        elif sql_upper.startswith('DROP'):
            return "DROP"
        else:
            return "SELECT"  # Default fallback
    
    def _parse_select_query(self, sql_text: str, query: ParsedQuery):
        """Enhanced SELECT query parsing using parent methods."""
        self._extract_select_columns(sql_text, query)
        self._extract_from_tables(sql_text, query)
        self._extract_where_conditions(sql_text, query)
        self._extract_joins(sql_text, query)
        self._extract_group_by(sql_text, query)
        self._extract_order_by(sql_text, query)
    
    def _parse_update_query(self, sql_text: str, query: ParsedQuery):
        """Parse UPDATE query."""
        update_pattern = r'UPDATE\s+(\w+)\s+SET\s+(.*?)(?:\s+WHERE\s+(.*?))?(?:\s*$)'
        match = re.search(update_pattern, sql_text, re.IGNORECASE | re.DOTALL)
        
        if match:
            query.update_table = match.group(1).strip()
            if match.group(3):
                query.where_conditions = [match.group(3).strip()]
    
    def _parse_delete_query(self, sql_text: str, query: ParsedQuery):
        """Parse DELETE query."""
        delete_pattern = r'DELETE\s+FROM\s+(\w+)(?:\s+WHERE\s+(.*?))?(?:\s*$)'
        match = re.search(delete_pattern, sql_text, re.IGNORECASE | re.DOTALL)
        
        if match:
            query.delete_table = match.group(1).strip()
            if match.group(2):
                query.where_conditions = [match.group(2).strip()]
    
    def _parse_create_database_query(self, sql_text: str, query: ParsedQuery):
        """Parse CREATE DATABASE query."""
        db_pattern = r'CREATE\s+DATABASE\s+(\w+)'
        match = re.search(db_pattern, sql_text, re.IGNORECASE)
        
        if match:
            query.create_database = match.group(1).strip()
    
    def _parse_drop_query(self, sql_text: str, query: ParsedQuery):
        """Parse DROP query."""
        drop_pattern = r'DROP\s+(\w+)\s+(\w+)'
        match = re.search(drop_pattern, sql_text, re.IGNORECASE)
        
        if match:
            query.drop_type = match.group(1).upper()
            query.drop_object = match.group(2).strip()
