# SQL to Spark Converter - Project Summary

## Overview
A comprehensive Python tool developed for the hackathon (Squad 06) that automatically converts Oracle/PostgreSQL SQL queries to Apache Spark code, supporting both PySpark DataFrame API and SparkSQL formats.

## 🎯 Project Goals
- **Primary Goal**: Convert legacy SQL queries to modern Apache Spark code
- **Target Users**: Data engineers migrating from traditional databases to Spark
- **Key Value**: Accelerate migration process and reduce manual conversion errors

## 🏗️ Architecture

### Core Components
```
sql_to_spark_converter/
├── src/
│   ├── __init__.py              # Main converter class
│   ├── parser/
│   │   ├── __init__.py          # Parser module exports
│   │   └── sql_parser.py        # SQL parsing logic using sqlparse
│   └── generators/
│       ├── __init__.py          # Generator module exports
│       ├── pyspark_generator.py # PySpark DataFrame API generator
│       └── sparksql_generator.py# SparkSQL code generator
├── main.py                      # CLI interface
├── examples/                    # Sample SQL files
├── test_converter.py           # Test suite
└── demo.py                     # Interactive demonstration
```

## 🔧 Technical Implementation

### SQL Parser (`sql_parser.py`)
- Uses `sqlparse` library for robust SQL parsing
- Extracts key components: SELECT, FROM, WHERE, JOIN, GROUP BY, HAVING, ORDER BY
- Handles complex queries with subqueries, window functions, and aggregations
- Identifies Oracle/PostgreSQL specific functions for conversion

### PySpark Generator (`pyspark_generator.py`)
- Converts parsed SQL to PySpark DataFrame API calls
- Generates complete Python code with proper imports
- Handles JOIN operations, filtering, grouping, and ordering
- Produces executable PySpark code

### SparkSQL Generator (`sparksql_generator.py`)
- Converts Oracle/PostgreSQL SQL to Spark-compatible SQL
- Function mapping (NVL → COALESCE, SUBSTR → SUBSTRING, etc.)
- Date literal conversion and ROWNUM handling
- Maintains SQL readability with proper formatting

## 🚀 Key Features

### ✅ Supported SQL Features
- **SELECT statements** with complex projections
- **JOINs** (INNER, LEFT, RIGHT, FULL OUTER)
- **WHERE clauses** with complex conditions
- **GROUP BY and HAVING** clauses
- **ORDER BY** with multiple columns and ASC/DESC
- **Aggregate functions** (COUNT, SUM, AVG, MIN, MAX)
- **Window functions** (ROW_NUMBER, RANK, etc.)
- **CASE statements** and conditional logic
- **Subqueries** and Common Table Expressions (CTEs)

### 🔄 Oracle/PostgreSQL Function Conversion
- `NVL` → `COALESCE`
- `SUBSTR` → `SUBSTRING`
- `SYSDATE` → `CURRENT_TIMESTAMP()`
- `ROWNUM` → `ROW_NUMBER() OVER (ORDER BY 1)`
- Date literals: `DATE 'YYYY-MM-DD'` → `DATE('YYYY-MM-DD')`

## 💻 Usage Examples

### CLI Usage
```bash
# Convert from file to both formats
python main.py -f query.sql --output both

# Convert query string to PySpark only
python main.py -q "SELECT * FROM users WHERE age > 25" --output pyspark

# Save output to file
python main.py -f complex_query.sql --output both -o converted_code.py
```

### Programmatic Usage
```python
from src import SQLToSparkConverter

converter = SQLToSparkConverter()

# Generate PySpark DataFrame API code
pyspark_code = converter.to_pyspark(sql_query)

# Generate SparkSQL code
sparksql_code = converter.to_sparksql(
