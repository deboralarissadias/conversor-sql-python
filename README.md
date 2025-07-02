# SQL to Spark Converter

A comprehensive tool for converting legacy SQL queries (Oracle/PostgreSQL style) to Apache Spark code using both PySpark DataFrame API and SparkSQL.

## Features

- **SQL Parsing**: Robust parsing of SQL queries using sqlparse
- **Dual Output**: Generates both PySpark DataFrame and SparkSQL code
- **CLI Interface**: Easy-to-use command-line interface
- **Web Interface**: Browser-based UI for easy conversion
- **Validation**: Built-in testing and validation system
- **Extensible**: Modular architecture for easy extension

## Installation

```bash
pip install -r requirements.txt
```

## Usage

### Web Interface

The easiest way to use the converter is through the web interface:

```bash
# Start the web application
python app.py
```

Then open your browser and navigate to `http://localhost:5000`

### Command Line Interface

```bash
# Convert a single SQL file
python main.py --input examples/simple_query.sql --output pyspark

# Convert SQL directly from command line
python main.py --query "SELECT * FROM users WHERE age > 25" --output sparksql

# Convert to both formats
python main.py --input examples/complex_query.sql --output both
```

### Python API

```python
from src import SQLToSparkConverter

converter = SQLToSparkConverter()

# Convert to PySpark
pyspark_code = converter.to_pyspark("SELECT * FROM users WHERE age > 25")

# Convert to SparkSQL
sparksql_code = converter.to_sparksql("SELECT * FROM users WHERE age > 25")
```

## Example Conversions

### Input (SQL)
```sql
SELECT nome, salario 
FROM funcionarios 
WHERE salario > 5000 
ORDER BY salario DESC
```

### Output (PySpark)
```python
funcionarios_df.filter("salario > 5000").select("nome", "salario").orderBy("salario", ascending=False)
```

### Output (SparkSQL)
```python
spark.sql("""
 SELECT nome, salario
FROM funcionarios
WHERE salario > 5000
ORDER BY salario DESC
""")
```

## Supported SQL Features

- SELECT statements with complex projections
- JOINs (INNER, LEFT, RIGHT, FULL OUTER)
- WHERE clauses with complex conditions
- GROUP BY and HAVING clauses
- ORDER BY with multiple columns
- Subqueries and CTEs
- Window functions
- Aggregate functions
- CASE statements

## Project Structure

```
sql_to_spark_converter/
├── src/
│   ├── __init__.py
│   ├── parser/
│   │   ├── __init__.py
│   │   ├── sql_parser.py
│   │   └── query_analyzer.py
│   ├── generators/
│   │   ├── __init__.py
│   │   ├── pyspark_generator.py
│   │   └── sparksql_generator.py
│   ├── validators/
│   │   ├── __init__.py
│   │   ├── syntax_validator.py
│   │   └── result_validator.py
│   └── utils/
│       ├── __init__.py
│       ├── templates.py
│       └── helpers.py
├── static/
│   ├── style.css
│   └── script.js
├── templates/
│   └── index.html
├── examples/
│   ├── simple_query.sql
│   ├── complex_query.sql
│   └── join_query.sql
├── app.py
├── main.py
├── demo.py
├── test_converter.py
├── requirements.txt
└── README.md
```

## Quick Start for New Users

1. Clone the repository
2. Install dependencies: `pip install -r requirements.txt`
3. Start the web interface: `python app.py`
4. Open your browser at `http://localhost:5000`
5. Enter a SQL query or upload a SQL file
6. View the converted PySpark and SparkSQL code

## License

MIT License