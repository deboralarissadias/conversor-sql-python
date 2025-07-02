"""
SparkSQL Code Generator

Generates SparkSQL code from parsed SQL queries with Spark-specific optimizations.
"""

from typing import List, Dict, Any
from ..parser.sql_parser import ParsedQuery


class SparkSQLGenerator:
    """Generates SparkSQL code from ParsedQuery objects."""
    
    def __init__(self):
        self.spark_functions = {
            'SUBSTR': 'SUBSTRING',
            'NVL': 'COALESCE',
            'DECODE': 'CASE',
            'ROWNUM': 'ROW_NUMBER()',
            'SYSDATE': 'CURRENT_TIMESTAMP()'
        }
    
    def generate(self, parsed_query: ParsedQuery) -> str:
        """Generate SparkSQL code from parsed query."""
        # Handle different query types
        if parsed_query.query_type == "INSERT":
            return self._generate_insert(parsed_query)
        elif parsed_query.query_type == "CREATE_TABLE":
            return self._generate_create_table(parsed_query)
        else:
            # Default to SELECT query handling
            return self._generate_select(parsed_query)
    
    def _generate_select(self, parsed_query: ParsedQuery) -> str:
        """Generate SparkSQL code for SELECT queries."""
        # Build the SQL query
        sql_query = self._build_sql_query(parsed_query)
        
        # Remove the semicolon from the end if present
        sql_query = sql_query.rstrip(';')
        
        # Wrap in spark.sql() with proper formatting
        return f'spark.sql("""\n {sql_query}\n""")'
    
    def _generate_insert(self, parsed_query: ParsedQuery) -> str:
        """Generate SparkSQL code for INSERT queries."""
        table_name = parsed_query.insert_table
        
        # Build the INSERT statement
        if parsed_query.insert_columns:
            columns_str = ", ".join(parsed_query.insert_columns)
            values_str = ", ".join(parsed_query.insert_values)
            insert_sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({values_str})"
        else:
            values_str = ", ".join(parsed_query.insert_values)
            insert_sql = f"INSERT INTO {table_name} VALUES ({values_str})"
        
        # Wrap in spark.sql() with proper formatting
        return f'spark.sql("""\n {insert_sql}\n""")'
    
    def _generate_create_table(self, parsed_query: ParsedQuery) -> str:
        """Generate SparkSQL code for CREATE TABLE queries."""
        table_name = parsed_query.create_table
        
        # Build column definitions
        column_defs = []
        for col_def in parsed_query.column_definitions:
            col_name = col_def["name"]
            col_type = col_def["type"]
            
            # Add constraints
            constraints = ""
            if "constraints" in col_def and col_def["constraints"]:
                constraints = " " + " ".join(col_def["constraints"])
            
            column_defs.append(f"{col_name} {col_type}{constraints}")
        
        columns_str = ",\n  ".join(column_defs)
        
        create_sql = f"CREATE TABLE {table_name} (\n  {columns_str}\n)"
        
        # Wrap in spark.sql() with proper formatting
        return f'spark.sql("""\n{create_sql}\n""")'
    
    def _build_sql_query(self, query: ParsedQuery) -> str:
        """Build the complete SQL query."""
        query_parts = []
        
        # SELECT clause
        select_clause = self._build_select_clause(query.select_columns)
        query_parts.append(select_clause)
        
        # FROM clause
        if query.from_tables:
            from_clause = self._build_from_clause(query.from_tables)
            query_parts.append(from_clause)
        
        # JOIN clauses
        for join in query.joins:
            join_clause = self._build_join_clause(join)
            query_parts.append(join_clause)
        
        # WHERE clause
        if query.where_conditions:
            where_clause = self._build_where_clause(query.where_conditions)
            query_parts.append(where_clause)
        
        # GROUP BY clause
        if query.group_by_columns:
            groupby_clause = self._build_group_by_clause(query.group_by_columns)
            query_parts.append(groupby_clause)
        
        # HAVING clause
        if query.having_conditions:
            having_clause = self._build_having_clause(query.having_conditions)
            query_parts.append(having_clause)
        
        # ORDER BY clause
        if query.order_by_columns:
            orderby_clause = self._build_order_by_clause(query.order_by_columns)
            query_parts.append(orderby_clause)
        
        return "\n".join(query_parts)
    
    def _build_select_clause(self, columns: List[str]) -> str:
        """Build SELECT clause."""
        if not columns:
            return "SELECT *"
        
        # Convert Oracle/PostgreSQL functions to Spark equivalents
        converted_columns = [self._convert_functions(col.strip()) for col in columns]
        
        return f"SELECT {', '.join(converted_columns)}"
    
    def _build_from_clause(self, tables: List[str]) -> str:
        """Build FROM clause."""
        if len(tables) == 1:
            return f"FROM {tables[0]}"
        
        # Multiple tables (comma-separated joins)
        return f"FROM {', '.join(tables)}"
    
    def _build_join_clause(self, join_info: Dict[str, str]) -> str:
        """Build JOIN clause."""
        join_type = join_info.get('type', 'JOIN')
        table = join_info.get('table', 'table2')
        condition = join_info.get('condition', '1=1')
        
        return f"{join_type} {table} ON {condition}"
    
    def _build_where_clause(self, conditions: List[str]) -> str:
        """Build WHERE clause."""
        if len(conditions) == 1:
            return f"WHERE {conditions[0]}"
        
        return f"WHERE {' AND '.join(conditions)}"
    
    def _build_group_by_clause(self, columns: List[str]) -> str:
        """Build GROUP BY clause."""
        return f"GROUP BY {', '.join(columns)}"
    
    def _build_having_clause(self, conditions: List[str]) -> str:
        """Build HAVING clause."""
        if len(conditions) == 1:
            return f"HAVING {conditions[0]}"
        
        return f"HAVING {' AND '.join(conditions)}"
    
    def _build_order_by_clause(self, columns: List[str]) -> str:
        """Build ORDER BY clause."""
        return f"ORDER BY {', '.join(columns)}"
    
    def _convert_functions(self, expression: str) -> str:
        """Convert Oracle/PostgreSQL functions to Spark SQL equivalents."""
        converted = expression
        
        for oracle_func, spark_func in self.spark_functions.items():
            # Simple replacement - can be enhanced with regex for more precision
            converted = converted.replace(oracle_func, spark_func)
        
        return converted