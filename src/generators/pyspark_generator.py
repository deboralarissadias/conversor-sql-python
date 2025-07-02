"""
PySpark DataFrame API Code Generator

Generates PySpark DataFrame API code from parsed SQL queries.
"""

import re
from typing import List, Dict, Any
from ..parser.sql_parser import ParsedQuery


class PySparkGenerator:
    """Generates PySpark DataFrame API code from ParsedQuery objects."""
    
    def __init__(self):
        pass
    
    def generate(self, parsed_query: ParsedQuery) -> str:
        """Generate concise PySpark code from parsed query."""
        # Handle different query types
        if parsed_query.query_type == "INSERT":
            return self._generate_insert(parsed_query)
        elif parsed_query.query_type == "CREATE_TABLE":
            return self._generate_create_table(parsed_query)
        else:
            # Default to SELECT query handling
            return self._generate_select(parsed_query)
    
    def _generate_select(self, parsed_query: ParsedQuery) -> str:
        """Generate PySpark code for SELECT queries."""
        # Get base table name for DataFrame variable
        base_table = "df"
        if parsed_query.from_tables:
            table_name = parsed_query.from_tables[0].split(" ")[0].lower()
            base_table = f"{table_name}"
        
        operations = []
        
        # Add JOIN operations
        for join in parsed_query.joins:
            join_type = join.get('type', 'JOIN').upper()
            join_table = join.get('table', '').split(" ")[0].lower()
            join_condition = join.get('condition', '1=1')
            
            # Convert join type to PySpark join type
            if 'LEFT' in join_type:
                spark_join_type = '"left"'
            elif 'RIGHT' in join_type:
                spark_join_type = '"right"'
            elif 'FULL' in join_type:
                spark_join_type = '"full"'
            else:
                spark_join_type = '"inner"'
            
            # Format the join condition for PySpark
            # This is a simplified version - in a real implementation, you'd need more sophisticated parsing
            condition = join_condition.replace('=', '==')
            operations.append(f'join({join_table}, {base_table}.{condition}, {spark_join_type})')
        
        # Add WHERE conditions
        if parsed_query.where_conditions:
            if len(parsed_query.where_conditions) == 1:
                condition = parsed_query.where_conditions[0]
                # Convert SQL boolean literals to Python
                condition = condition.replace(" TRUE", " True").replace(" FALSE", " False")
                operations.append(f'filter({base_table}.{condition})')
            else:
                condition_str = " AND ".join(f"({cond})" for cond in parsed_query.where_conditions)
                condition_str = condition_str.replace(" TRUE", " True").replace(" FALSE", " False")
                operations.append(f'filter("{condition_str}")')
        
        # Add GROUP BY
        if parsed_query.group_by_columns:
            cols = ", ".join(f'{base_table}.{col.strip()}' for col in parsed_query.group_by_columns)
            operations.append(f'groupBy({cols})')
            
            # Add aggregations if there are any
            if parsed_query.aggregate_functions:
                aggs = []
                for agg in parsed_query.aggregate_functions:
                    if "AVG(" in agg.upper():
                        col_name = agg.upper().split("AVG(")[1].split(")")[0].strip()
                        alias = agg.split(" AS ")[-1].strip() if " AS " in agg else f"avg_{col_name}"
                        aggs.append(f'avg({col_name}).alias("{alias}")')
                    elif "SUM(" in agg.upper():
                        col_name = agg.upper().split("SUM(")[1].split(")")[0].strip()
                        alias = agg.split(" AS ")[-1].strip() if " AS " in agg else f"sum_{col_name}"
                        aggs.append(f'sum({col_name}).alias("{alias}")')
                    elif "COUNT(" in agg.upper():
                        col_name = agg.upper().split("COUNT(")[1].split(")")[0].strip()
                        alias = agg.split(" AS ")[-1].strip() if " AS " in agg else "count"
                        aggs.append(f'count({col_name}).alias("{alias}")')
                
                if aggs:
                    operations.append(f'agg({", ".join(aggs)})')
        
        # Add SELECT (projection)
        if parsed_query.select_columns and not (len(parsed_query.select_columns) == 1 and parsed_query.select_columns[0].strip() == '*'):
            # Process each column to handle aliases and functions
            processed_cols = []
            for col in parsed_query.select_columns:
                col = col.strip()
                if " AS " in col.upper():
                    parts = col.split(" AS ", 1)
                    expr = parts[0].strip()
                    alias = parts[1].strip()
                    
                    # Handle COALESCE function
                    if "COALESCE(" in expr.upper():
                        args = expr.split("COALESCE(")[1].rstrip(")").split(",")
                        args = [arg.strip() for arg in args]
                        if "AVG(" in args[0].upper():
                            inner_col = args[0].split("AVG(")[1].split(")")[0].strip()
                            processed_cols.append(f'coalesce(avg({inner_col}), {args[1]}).alias("{alias}")')
                        elif "SUM(" in args[0].upper():
                            inner_col = args[0].split("SUM(")[1].split(")")[0].strip()
                            processed_cols.append(f'coalesce(sum({inner_col}), {args[1]}).alias("{alias}")')
                        else:
                            processed_cols.append(f'coalesce({", ".join(args)}).alias("{alias}")')
                    else:
                        processed_cols.append(f'col("{expr}").alias("{alias}")')
                else:
                    processed_cols.append(f'"{col}"')
            
            if processed_cols:
                operations.append(f'select({", ".join(processed_cols)})')
        
        # Add ORDER BY
        if parsed_query.order_by_columns:
            order_exprs = []
            for col in parsed_query.order_by_columns:
                col = col.strip().rstrip(';')  # Remove semicolon if present
                if ' DESC' in col.upper():
                    col_name = col.replace(' DESC', '').replace(' desc', '').strip()
                    order_exprs.append(f'col("{col_name}").desc()')
                elif ' ASC' in col.upper():
                    col_name = col.replace(' ASC', '').replace(' asc', '').strip()
                    order_exprs.append(f'col("{col_name}")')
                else:
                    # Default to ASC if no direction specified
                    order_exprs.append(f'col("{col}")')
            
            order_str = ", ".join(order_exprs)
            operations.append(f'orderBy({order_str})')
        
        # Join all operations with dot notation
        result = base_table
        if operations:
            result += "." + ".".join(operations)
        
        return result
    
    def _generate_insert(self, parsed_query: ParsedQuery) -> str:
        """Generate PySpark code for INSERT queries."""
        table_name = parsed_query.insert_table
        df_name = f"{table_name.lower()}"
        
        # Create a concise one-liner for INSERT
        values_str = ", ".join(parsed_query.insert_values)
        
        if parsed_query.insert_columns:
            columns_str = ", ".join(f'"{col}"' for col in parsed_query.insert_columns)
            return f"{df_name}.union(spark.createDataFrame([({values_str})], {df_name}.schema))"
        else:
            return f"{df_name}.union(spark.createDataFrame([({values_str})], {df_name}.schema))"
    
    def _generate_create_table(self, parsed_query: ParsedQuery) -> str:
        """Generate PySpark code for CREATE TABLE queries."""
        table_name = parsed_query.create_table
        
        # Create a concise one-liner for CREATE TABLE
        schema_parts = []
        for col_def in parsed_query.column_definitions:
            col_name = col_def["name"]
            col_type = self._convert_sql_type_to_spark(col_def["type"])
            nullable = "True" if "NOT NULL" not in col_def.get("constraints", []) else "False"
            schema_parts.append(f'StructField("{col_name}", {col_type}, {nullable})')
        
        schema_str = ", ".join(schema_parts)
        
        return f"spark.createDataFrame([], StructType([{schema_str}])).write.saveAsTable(\"{table_name}\")"
    
    def _convert_sql_type_to_spark(self, sql_type: str) -> str:
        """Convert SQL data type to PySpark data type."""
        sql_type = sql_type.upper()
        
        if "INT" in sql_type:
            return "IntegerType()"
        elif "CHAR" in sql_type or "VARCHAR" in sql_type or "TEXT" in sql_type:
            return "StringType()"
        elif "FLOAT" in sql_type or "DOUBLE" in sql_type or "DECIMAL" in sql_type or "NUMERIC" in sql_type:
            return "DoubleType()"
        elif "BOOL" in sql_type:
            return "BooleanType()"
        elif "DATE" in sql_type:
            return "DateType()"
        elif "TIMESTAMP" in sql_type or "DATETIME" in sql_type:
            return "TimestampType()"
        else:
            # Default to string for unknown types
            return "StringType()"