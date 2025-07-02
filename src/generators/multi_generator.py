"""
Multi-Query Generator

Enhanced generator that can handle multiple SQL queries and more query types.
"""

from typing import List, Dict, Any
from ..parser.sql_parser import ParsedQuery
from .pyspark_generator import PySparkGenerator
from .sparksql_generator import SparkSQLGenerator


class MultiPySparkGenerator(PySparkGenerator):
    """Enhanced PySpark generator for multiple queries and more query types."""
    
    def generate_multiple(self, parsed_queries: List[ParsedQuery]) -> str:
        """Generate PySpark code for multiple queries."""
        if not parsed_queries:
            return "# No queries to process"
        
        code_blocks = []
        code_blocks.append("from pyspark.sql import SparkSession")
        code_blocks.append("from pyspark.sql.functions import col, avg, sum, count, max, min, coalesce, lit")
        code_blocks.append("")
        code_blocks.append("spark = SparkSession.builder.appName(\"SQL to PySpark\").getOrCreate()")
        code_blocks.append("")
        
        # Track tables for all queries
        all_tables = set()
        
        # First pass to collect all tables
        for query in parsed_queries:
            if hasattr(query, 'from_tables') and query.from_tables:
                for table_ref in query.from_tables:
                    table_name = table_ref.split(" ")[0].strip()
                    all_tables.add(table_name)
            
            if hasattr(query, 'joins') and query.joins:
                for join in query.joins:
                    table_name = join.get('table', '').split(" ")[0].strip()
                    if table_name:
                        all_tables.add(table_name)
        
        # Create DataFrame variables for all tables
        for table in sorted(all_tables):
            var_name = table.lower()
            code_blocks.append(f"{var_name} = spark.table(\"{table}\")")
        
        if all_tables:
            code_blocks.append("")
        
        # Generate code for each query
        for i, query in enumerate(parsed_queries, 1):
            if query.query_type == "ERROR":
                code_blocks.append(f"# Query {i}: Error - {query.error_message}")
                code_blocks.append(f"# Original: {query.original_query}")
            else:
                if i > 1:
                    code_blocks.append(f"# Query {i}: {query.query_type}")
                
                try:
                    generated_code = self._format_pyspark_code(self.generate(query))
                    if i == 1:
                        code_blocks.append(f"result = {generated_code}")
                        code_blocks.append("")
                        code_blocks.append("result.show()")
                    else:
                        code_blocks.append(f"result_{i} = {generated_code}")
                        code_blocks.append("")
                        code_blocks.append(f"result_{i}.show()")
                except Exception as e:
                    code_blocks.append(f"# Error generating code: {str(e)}")
            
            if i < len(parsed_queries):
                code_blocks.append("")
        
        return "\n".join(code_blocks)
    
    def _format_pyspark_code(self, code: str) -> str:
        """Format PySpark code with proper indentation for readability."""
        if not code or "." not in code:
            return code
        
        # Split by dot and method calls
        parts = []
        current = ""
        paren_level = 0
        
        for char in code:
            if char == '(':
                paren_level += 1
            elif char == ')':
                paren_level -= 1
            
            current += char
            
            if char == '.' and paren_level == 0:
                parts.append(current[:-1])  # Remove the dot
                current = "."
        
        if current:
            parts.append(current)
        
        # Format with indentation
        if len(parts) <= 1:
            return code
        
        formatted = ["(" + parts[0]]
        for part in parts[1:]:
            formatted.append("          " + part)
        
        return "\n".join(formatted) + ")"
    
    def generate(self, parsed_query: ParsedQuery) -> str:
        """Enhanced generate method for more query types."""
        if parsed_query.query_type == "UPDATE":
            return self._generate_update(parsed_query)
        elif parsed_query.query_type == "DELETE":
            return self._generate_delete(parsed_query)
        elif parsed_query.query_type == "CREATE_DATABASE":
            return self._generate_create_database(parsed_query)
        elif parsed_query.query_type == "DROP":
            return self._generate_drop(parsed_query)
        else:
            # Use parent class for SELECT, INSERT, CREATE_TABLE
            return super().generate(parsed_query)
    
    def _generate_update(self, parsed_query: ParsedQuery) -> str:
        """Generate PySpark code for UPDATE queries."""
        table_name = parsed_query.update_table
        df_name = f"{table_name.lower()}"
        
        # For UPDATE, we need to use withColumn or select with when/otherwise
        if parsed_query.where_conditions:
            condition = parsed_query.where_conditions[0]
            return f"{df_name}.filter(\"{condition}\")"
        else:
            return f"{df_name}"
    
    def _generate_delete(self, parsed_query: ParsedQuery) -> str:
        """Generate PySpark code for DELETE queries."""
        table_name = parsed_query.delete_table
        df_name = f"{table_name.lower()}"
        
        if parsed_query.where_conditions:
            condition = parsed_query.where_conditions[0]
            # For DELETE, we filter out the rows (keep the opposite condition)
            return f"{df_name}.filter(\"NOT ({condition})\")"
        else:
            # DELETE without WHERE would remove all rows
            return f"spark.createDataFrame([], {df_name}.schema)"
    
    def _generate_create_database(self, parsed_query: ParsedQuery) -> str:
        """Generate PySpark code for CREATE DATABASE queries."""
        db_name = parsed_query.create_database
        return f"spark.sql(\"CREATE DATABASE {db_name}\")"
    
    def _generate_drop(self, parsed_query: ParsedQuery) -> str:
        """Generate PySpark code for DROP queries."""
        drop_type = parsed_query.drop_type
        drop_object = parsed_query.drop_object
        return f"spark.sql(\"DROP {drop_type} {drop_object}\")"


class MultiSparkSQLGenerator(SparkSQLGenerator):
    """Enhanced SparkSQL generator for multiple queries."""
    
    def generate_multiple(self, parsed_queries: List[ParsedQuery]) -> str:
        """Generate SparkSQL code for multiple queries."""
        if not parsed_queries:
            return "-- No queries to process"
        
        code_blocks = []
        code_blocks.append("-- SparkSQL code for multiple queries")
        code_blocks.append("")
        
        for i, query in enumerate(parsed_queries, 1):
            if query.query_type == "ERROR":
                code_blocks.append(f"-- Query {i}: Error - {query.error_message}")
                code_blocks.append(f"-- Original: {query.original_query}")
            else:
                if i > 1:
                    code_blocks.append(f"-- Query {i}: {query.query_type}")
                try:
                    generated_code = self.generate(query)
                    code_blocks.append(generated_code)
                except Exception as e:
                    code_blocks.append(f"-- Error generating code: {str(e)}")
            code_blocks.append("")
        
        return "\n".join(code_blocks)