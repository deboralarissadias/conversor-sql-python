"""
Helper functions for SQL to Spark Converter
"""

import re
from typing import Dict, List, Any


def clean_sql_string(sql: str) -> str:
    """Clean and normalize SQL query string."""
    # Remove SQL comments (-- style)
    sql = re.sub(r'--.*$', '', sql, flags=re.MULTILINE)
    # Remove extra whitespace and normalize
    sql = re.sub(r'\s+', ' ', sql.strip())
    return sql


def format_column_name(column_name: str) -> str:
    """Format column name for Spark compatibility."""
    # Remove quotes if present
    if column_name.startswith('"') and column_name.endswith('"'):
        column_name = column_name[1:-1]
    elif column_name.startswith("'") and column_name.endswith("'"):
        column_name = column_name[1:-1]
    
    return column_name


def format_table_name(table_name: str) -> str:
    """Format table name for Spark compatibility."""
    # Remove quotes if present
    if table_name.startswith('"') and table_name.endswith('"'):
        table_name = table_name[1:-1]
    elif table_name.startswith("'") and table_name.endswith("'"):
        table_name = table_name[1:-1]
    
    return table_name


def indent_text(text: str, spaces: int = 4) -> str:
    """Indent each line of text by specified number of spaces."""
    indent = ' ' * spaces
    return indent + text.replace('\n', '\n' + indent)