"""
SQL Syntax Validator

Validates SQL syntax before conversion.
"""

import re
import sqlparse
from typing import Tuple, List


class SyntaxValidator:
    """Validates SQL syntax before conversion."""
    
    def __init__(self):
        self.common_errors = {
            'unbalanced_parentheses': r'\([^()]*(?:\([^()]*\)[^()]*)*$',
            'missing_from': r'SELECT\s+.*(?!\sFROM\s)$',
            'invalid_join': r'JOIN\s+.*(?!\sON\s)',
            'unclosed_quotes': r'\'[^\']*$|\"[^\"]*$'
        }
    
    def validate(self, sql_query: str) -> Tuple[bool, List[str]]:
        """
        Validate SQL syntax and return validation status and errors.
        
        Args:
            sql_query: SQL query string to validate
            
        Returns:
            Tuple containing:
                - Boolean indicating if validation passed
                - List of error messages (empty if validation passed)
        """
        errors = []
        
        # Check for empty query
        if not sql_query or not sql_query.strip():
            errors.append("Empty SQL query")
            return False, errors
        
        # Basic structure validation
        if not sql_query.strip().upper().startswith("SELECT"):
            errors.append("Query must start with SELECT")
        
        # Check for common syntax errors
        for error_name, pattern in self.common_errors.items():
            if re.search(pattern, sql_query, re.IGNORECASE | re.DOTALL):
                errors.append(f"Possible syntax error: {error_name.replace('_', ' ')}")
        
        # Use sqlparse for more comprehensive validation
        try:
            parsed = sqlparse.parse(sql_query)
            if not parsed:
                errors.append("Failed to parse SQL query")
            
            # Check for balanced parentheses
            if sql_query.count('(') != sql_query.count(')'):
                errors.append("Unbalanced parentheses")
                
        except Exception as e:
            errors.append(f"SQL parsing error: {str(e)}")
        
        return len(errors) == 0, errors