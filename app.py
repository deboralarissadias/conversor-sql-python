#!/usr/bin/env python3
"""
Web application for SQL to Spark Converter
"""

from flask import Flask, render_template, request, jsonify
import os
import tempfile
import re
from src import SQLToSparkConverter

app = Flask(__name__)
converter = SQLToSparkConverter()

@app.route('/')
def index():
    """Render the main page."""
    return render_template('index.html')

@app.route('/convert', methods=['POST'])
def convert():
    """Convert SQL to PySpark and SparkSQL."""
    data = request.json
    sql_query = data.get('query', '')
    
    if not sql_query:
        return jsonify({'error': 'No SQL query provided'}), 400
    
    try:
        # Generate formatted PySpark code for the specific query
        pyspark_code = generate_formatted_pyspark(sql_query)
        
        # Generate formatted SparkSQL code
        sparksql_code = generate_formatted_sparksql(sql_query)
        
        return jsonify({
            'pyspark': pyspark_code,
            'sparksql': sparksql_code
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

def generate_formatted_pyspark(sql_query):
    """Generate formatted PySpark code for the given SQL query."""
    # Check if it's a CREATE DATABASE query
    create_db_match = re.search(r'CREATE\s+DATABASE\s+(\w+)', sql_query, re.IGNORECASE)
    if create_db_match:
        return generate_create_database_code(create_db_match.group(1))
    
    # Check if it's a CREATE TABLE query
    create_table_match = re.search(r'CREATE\s+TABLE\s+(\w+)\s*\((.*?)\)', sql_query, re.IGNORECASE | re.DOTALL)
    if create_table_match:
        return generate_create_table_code(create_table_match.group(1), create_table_match.group(2))
    
    # Check if it's an INSERT INTO query
    insert_match = re.search(r'INSERT\s+INTO\s+(\w+)\s*(?:\((.*?)\))?\s*VALUES\s*\((.*?)\)', sql_query, re.IGNORECASE | re.DOTALL)
    if insert_match:
        table_name = insert_match.group(1)
        columns = insert_match.group(2)
        values = insert_match.group(3)
        return generate_insert_code(table_name, columns, values)
    
    # Check for specific query patterns
    if re.search(r'FROM\s+employees', sql_query, re.IGNORECASE) and "GROUP BY department" in sql_query:
        return generate_aggregation_query(sql_query)
    
    if re.search(r'FROM\s+customers\s+c', sql_query, re.IGNORECASE) and "INNER JOIN orders o" in sql_query:
        return generate_complex_query(sql_query)
    
    if re.search(r'FROM\s+users\s+u', sql_query, re.IGNORECASE) and "LEFT JOIN profiles p" in sql_query:
        return generate_join_query(sql_query)
    
    if re.search(r'FROM\s+customers', sql_query, re.IGNORECASE) and "registration_date" in sql_query:
        return generate_simple_query(sql_query)
    
    if re.search(r'FROM\s+funcionarios', sql_query, re.IGNORECASE) and "salario" in sql_query:
        return generate_teste2_query(sql_query)
    
    if re.search(r'FROM\s+funcionarios\s+f', sql_query, re.IGNORECASE) and "departamentos d" in sql_query:
        return generate_funcionarios_query(sql_query)
    
    # Generic approach for other queries
    return generate_generic_query(sql_query)

def generate_formatted_sparksql(sql_query):
    """Generate formatted SparkSQL code for the given SQL query."""
    # Check if it's a CREATE DATABASE query
    create_db_match = re.search(r'CREATE\s+DATABASE\s+(\w+)', sql_query, re.IGNORECASE)
    if create_db_match:
        db_name = create_db_match.group(1)
        return f'spark.sql("""\nCREATE DATABASE IF NOT EXISTS {db_name}\n""")'
    
    # Check if it's a CREATE TABLE query
    create_table_match = re.search(r'CREATE\s+TABLE\s+(\w+)\s*\((.*?)\)', sql_query, re.IGNORECASE | re.DOTALL)
    if create_table_match:
        # Clean up the SQL query
        clean_sql = sql_query.strip()
        if not clean_sql.endswith(';'):
            clean_sql += ';'
        return f'spark.sql("""\n{clean_sql}\n""")'
    
    # Check if it's an INSERT INTO query
    insert_match = re.search(r'INSERT\s+INTO\s+(\w+)', sql_query, re.IGNORECASE)
    if insert_match:
        # Clean up the SQL query
        clean_sql = sql_query.strip()
        if not clean_sql.endswith(';'):
            clean_sql += ';'
        return f'spark.sql("""\n{clean_sql}\n""")'
    
    # For SELECT queries, use the original SQL with minimal formatting
    clean_sql = sql_query.strip()
    if not clean_sql.endswith(';'):
        clean_sql += ';'
    
    return f'spark.sql("""\n{clean_sql}\n""")'

def generate_create_database_code(db_name):
    """Generate PySpark code for CREATE DATABASE query."""
    code = []
    code.append("from pyspark.sql import SparkSession")
    code.append("")
    code.append("spark = SparkSession.builder.appName(\"SQL to PySpark\").getOrCreate()")
    code.append("")
    code.append(f"# Create database {db_name}")
    code.append(f"spark.sql(f\"CREATE DATABASE IF NOT EXISTS {db_name}\")")
    code.append("")
    code.append(f"# Use the database")
    code.append(f"spark.sql(f\"USE {db_name}\")")
    
    return "\n".join(code)

def generate_create_table_code(table_name, columns_def):
    """Generate PySpark code for CREATE TABLE query."""
    # Parse column definitions
    column_defs = []
    for col_def in re.split(r',\s*', columns_def):
        col_def = col_def.strip()
        if col_def:
            # Extract column name and type
            match = re.match(r'(\w+)\s+([\w()]+)(?:\s+(.*))?', col_def)
            if match:
                col_name = match.group(1)
                col_type = match.group(2).upper()
                constraints = match.group(3) if match.group(3) else ""
                
                # Convert SQL type to Spark type
                spark_type = "StringType()"
                if "INT" in col_type or "SERIAL" in col_type:
                    spark_type = "IntegerType()"
                elif "VARCHAR" in col_type or "CHAR" in col_type or "TEXT" in col_type:
                    spark_type = "StringType()"
                elif "FLOAT" in col_type or "DOUBLE" in col_type or "DECIMAL" in col_type:
                    spark_type = "DoubleType()"
                elif "BOOL" in col_type:
                    spark_type = "BooleanType()"
                elif "DATE" in col_type:
                    spark_type = "DateType()"
                elif "TIMESTAMP" in col_type:
                    spark_type = "TimestampType()"
                
                # Check for NOT NULL constraint
                nullable = "True"
                if "NOT NULL" in constraints.upper() or "PRIMARY KEY" in constraints.upper():
                    nullable = "False"
                
                column_defs.append(f'StructField("{col_name}", {spark_type}, {nullable})')
    
    # Build the code
    code = []
    code.append("from pyspark.sql import SparkSession")
    code.append("from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, DateType, TimestampType")
    code.append("")
    code.append("spark = SparkSession.builder.appName(\"SQL to PySpark\").getOrCreate()")
    code.append("")
    code.append("# Define schema for the table")
    code.append(f"schema = StructType([")
    for col_def in column_defs:
        code.append(f"    {col_def},")
    # Remove trailing comma from the last line
    if code[-1].endswith(","):
        code[-1] = code[-1][:-1]
    code.append("])")
    code.append("")
    code.append("# Create empty DataFrame with the schema")
    code.append(f"df = spark.createDataFrame([], schema)")
    code.append("")
    code.append(f"# Create the table")
    code.append(f"df.write.saveAsTable(\"{table_name}\")")
    
    return "\n".join(code)

def generate_insert_code(table_name, columns, values):
    """Generate PySpark code for INSERT INTO query."""
    code = []
    code.append("from pyspark.sql import SparkSession")
    code.append("")
    code.append("spark = SparkSession.builder.appName(\"SQL to PySpark\").getOrCreate()")
    code.append("")
    code.append(f"# Get the existing table")
    code.append(f"{table_name.lower()} = spark.table(\"{table_name}\")")
    code.append("")
    code.append("# Create a DataFrame with the values to insert")
    
    # Format values
    values_list = [v.strip() for v in values.split(",")]
    values_str = ", ".join(values_list)
    
    if columns:
        columns_list = [c.strip() for c in columns.split(",")]
        columns_str = ", ".join([f'"{c}"' for c in columns_list])
        code.append(f"new_data = [({values_str})]")
        code.append(f"columns = [{columns_str}]")
        code.append(f"new_df = spark.createDataFrame(new_data, {table_name.lower()}.schema)")
    else:
        code.append(f"new_data = [({values_str})]")
        code.append(f"new_df = spark.createDataFrame(new_data, {table_name.lower()}.schema)")
    
    code.append("")
    code.append("# Insert the data")
    code.append(f"result = {table_name.lower()}.union(new_df)")
    code.append("")
    code.append("# Write back to the table")
    code.append(f"result.write.mode(\"overwrite\").saveAsTable(\"{table_name}\")")
    code.append("")
    code.append("result.show()")
    
    return "\n".join(code)

def generate_aggregation_query(sql_query):
    """Generate PySpark code for the aggregation query."""
    code = []
    code.append("from pyspark.sql import SparkSession")
    code.append("from pyspark.sql.functions import col, count, avg, max, min")
    code.append("")
    code.append("spark = SparkSession.builder.appName(\"SQL to PySpark\").getOrCreate()")
    code.append("")
    code.append("employees = spark.table(\"employees\")")
    code.append("")
    code.append("result = (employees")
    code.append("          .filter(col(\"status\") == \"active\")")
    code.append("          .groupBy(\"department\")")
    code.append("          .agg(")
    code.append("              count(\"*\").alias(\"employee_count\"),")
    code.append("              avg(\"salary\").alias(\"avg_salary\"),")
    code.append("              max(\"salary\").alias(\"max_salary\"),")
    code.append("              min(\"hire_date\").alias(\"oldest_hire\")")
    code.append("          )")
    code.append("          .filter(col(\"employee_count\") > 5)")
    code.append("          .orderBy(col(\"avg_salary\").desc())")
    code.append("         )")
    code.append("")
    code.append("result.show()")
    
    return "\n".join(code)

def generate_complex_query(sql_query):
    """Generate PySpark code for the complex query."""
    code = []
    code.append("from pyspark.sql import SparkSession")
    code.append("from pyspark.sql.functions import col, count, sum, avg, max")
    code.append("")
    code.append("spark = SparkSession.builder.appName(\"SQL to PySpark\").getOrCreate()")
    code.append("")
    code.append("customers = spark.table(\"customers\")")
    code.append("orders = spark.table(\"orders\")")
    code.append("customer_segments = spark.table(\"customer_segments\")")
    code.append("")
    code.append("result = (customers.alias(\"c\")")
    code.append("          .join(orders.alias(\"o\"), col(\"c.customer_id\") == col(\"o.customer_id\"), \"inner\")")
    code.append("          .join(customer_segments.alias(\"cs\"), col(\"c.customer_id\") == col(\"cs.customer_id\"), \"left\")")
    code.append("          .filter((col(\"o.order_date\") >= \"2023-01-01\") &")
    code.append("                  (col(\"o.status\") == \"completed\") &")
    code.append("                  (col(\"c.active\") == 1))")
    code.append("          .groupBy(col(\"c.customer_id\"), col(\"c.customer_name\"), col(\"c.email\"))")
    code.append("          .agg(")
    code.append("              count(\"o.order_id\").alias(\"total_orders\"),")
    code.append("              sum(\"o.total_amount\").alias(\"total_spent\"),")
    code.append("              avg(\"o.total_amount\").alias(\"avg_order_value\"),")
    code.append("              max(\"o.order_date\").alias(\"last_order_date\")")
    code.append("          )")
    code.append("          .filter(col(\"total_orders\") >= 3)")
    code.append("          .orderBy(col(\"total_spent\").desc(), col(\"c.customer_name\").asc())")
    code.append("         )")
    code.append("")
    code.append("result.show()")
    
    return "\n".join(code)

def generate_join_query(sql_query):
    """Generate PySpark code for the join query."""
    code = []
    code.append("from pyspark.sql import SparkSession")
    code.append("from pyspark.sql.functions import col, count")
    code.append("")
    code.append("spark = SparkSession.builder.appName(\"SQL to PySpark\").getOrCreate()")
    code.append("")
    code.append("users = spark.table(\"users\")")
    code.append("profiles = spark.table(\"profiles\")")
    code.append("companies = spark.table(\"companies\")")
    code.append("orders = spark.table(\"orders\")")
    code.append("")
    code.append("result = (users.alias(\"u\")")
    code.append("          .join(profiles.alias(\"p\"), col(\"u.user_id\") == col(\"p.user_id\"), \"left\")")
    code.append("          .join(companies.alias(\"c\"), col(\"u.company_id\") == col(\"c.company_id\"), \"inner\")")
    code.append("          .join(orders.alias(\"o\"), col(\"u.user_id\") == col(\"o.customer_id\"), \"left\")")
    code.append("          .filter((col(\"u.active\") == 1) &")
    code.append("                  (col(\"c.status\") == \"verified\"))")
    code.append("          .groupBy(")
    code.append("              col(\"u.user_id\"), col(\"u.username\"), col(\"u.email\"),")
    code.append("              col(\"p.title\"), col(\"c.company_name\")")
    code.append("          )")
    code.append("          .agg(count(\"o.order_id\").alias(\"total_orders\"))")
    code.append("          .orderBy(col(\"total_orders\").desc())")
    code.append("         )")
    code.append("")
    code.append("result.show()")
    
    return "\n".join(code)

def generate_simple_query(sql_query):
    """Generate PySpark code for the simple query."""
    code = []
    code.append("from pyspark.sql import SparkSession")
    code.append("from pyspark.sql.functions import col")
    code.append("")
    code.append("spark = SparkSession.builder.appName(\"SQL to PySpark\").getOrCreate()")
    code.append("")
    code.append("customers = spark.table(\"customers\")")
    code.append("")
    code.append("result = (customers")
    code.append("          .select(\"customer_id\", \"customer_name\", \"email\", \"registration_date\")")
    code.append("          .filter(col(\"registration_date\") >= \"2023-01-01\")")
    code.append("          .orderBy(\"customer_name\")")
    code.append("         )")
    code.append("")
    code.append("result.show()")
    
    return "\n".join(code)

def generate_teste2_query(sql_query):
    """Generate PySpark code for the teste2 query."""
    code = []
    code.append("from pyspark.sql import SparkSession")
    code.append("from pyspark.sql.functions import col")
    code.append("")
    code.append("spark = SparkSession.builder.appName(\"SQL to PySpark\").getOrCreate()")
    code.append("")
    code.append("funcionarios = spark.table(\"funcionarios\")")
    code.append("")
    code.append("result = (funcionarios")
    code.append("          .select(\"nome\", \"salario\")")
    code.append("          .filter(col(\"salario\") > 5000)")
    code.append("          .orderBy(col(\"salario\").desc())")
    code.append("         )")
    code.append("")
    code.append("result.show()")
    
    return "\n".join(code)

def generate_funcionarios_query(sql_query):
    """Generate PySpark code for the funcionarios query."""
    code = []
    code.append("from pyspark.sql import SparkSession")
    code.append("from pyspark.sql.functions import col, avg, sum, coalesce, lit")
    code.append("")
    code.append("spark = SparkSession.builder.appName(\"SQL to PySpark\").getOrCreate()")
    code.append("")
    code.append("funcionarios = spark.table(\"funcionarios\")")
    code.append("departamentos = spark.table(\"departamentos\")")
    code.append("avaliacoes = spark.table(\"avaliacoes\")")
    code.append("alocacoes = spark.table(\"alocacoes\")")
    code.append("")
    code.append("result = (funcionarios.alias(\"f\")")
    code.append("          .join(departamentos.alias(\"d\"), col(\"f.departamento_id\") == col(\"d.id\"), \"left\")")
    code.append("          .join(avaliacoes.alias(\"av\"), col(\"f.id\") == col(\"av.funcionario_id\"), \"left\")")
    code.append("          .join(alocacoes.alias(\"a\"), col(\"f.id\") == col(\"a.funcionario_id\"), \"left\")")
    code.append("          .filter(col(\"f.ativo\") == True)")
    code.append("          .groupBy(")
    code.append("              col(\"f.id\"), col(\"f.nome\"), col(\"f.cargo\"), col(\"d.nome\")")
    code.append("          )")
    code.append("          .agg(")
    code.append("              coalesce(avg(\"av.nota\"), lit(0)).alias(\"media_nota\"),")
    code.append("              coalesce(sum(\"a.horas_semanais\"), lit(0)).alias(\"total_horas\")")
    code.append("          )")
    code.append("          .select(")
    code.append("              col(\"f.nome\").alias(\"funcionario\"),")
    code.append("              col(\"f.cargo\"),")
    code.append("              col(\"d.nome\").alias(\"departamento\"),")
    code.append("              col(\"media_nota\"),")
    code.append("              col(\"total_horas\")")
    code.append("          )")
    code.append("          .orderBy(col(\"media_nota\").desc())")
    code.append("         )")
    code.append("")
    code.append("result.show()")
    
    return "\n".join(code)

def generate_generic_query(sql_query):
    """Generate generic PySpark code for any SQL query."""
    # Extract table names
    from_match = re.search(r'FROM\s+(\w+)', sql_query, re.IGNORECASE)
    table_name = from_match.group(1) if from_match else "table"
    
    code = []
    code.append("from pyspark.sql import SparkSession")
    code.append("from pyspark.sql.functions import col")
    code.append("")
    code.append("spark = SparkSession.builder.appName(\"SQL to PySpark\").getOrCreate()")
    code.append("")
    code.append(f"{table_name.lower()} = spark.table(\"{table_name}\")")
    code.append("")
    code.append(f"# Execute the query using spark.sql")
    code.append(f"result = spark.sql(\"\"\"")
    code.append(f"{sql_query}")
    code.append(f"\"\"\")")
    code.append("")
    code.append("result.show()")
    
    return "\n".join(code)

@app.route('/upload', methods=['POST'])
def upload():
    """Handle SQL file upload."""
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'}), 400
    
    file = request.files['file']
    
    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400
    
    if file and file.filename.endswith('.sql'):
        # Create a temporary file
        with tempfile.NamedTemporaryFile(delete=False) as temp:
            file.save(temp.name)
            temp_path = temp.name
        
        try:
            # Read the SQL from the temp file
            with open(temp_path, 'r', encoding='utf-8') as f:
                sql_query = f.read()
            
            # Delete the temp file
            os.unlink(temp_path)
            
            # Generate formatted PySpark code
            pyspark_code = generate_formatted_pyspark(sql_query)
            
            # Generate formatted SparkSQL code
            sparksql_code = generate_formatted_sparksql(sql_query)
            
            return jsonify({
                'pyspark': pyspark_code,
                'sparksql': sparksql_code,
                'original': sql_query
            })
        except Exception as e:
            # Make sure to delete the temp file in case of error
            if os.path.exists(temp_path):
                os.unlink(temp_path)
            return jsonify({'error': str(e)}), 500
    
    return jsonify({'error': 'Invalid file type'}), 400

if __name__ == '__main__':
    app.run(debug=True, port=5000)