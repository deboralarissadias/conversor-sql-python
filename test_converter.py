#!/usr/bin/env python3
"""
Basic test script for SQL to Spark Converter
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from src import SQLToSparkConverter


def test_simple_query():
    """Test a simple SQL query conversion."""
    print("Testing Simple Query Conversion")
    print("=" * 50)
    
    sql_query = """
    SELECT customer_id, customer_name, email 
    FROM customers 
    WHERE registration_date >= '2023-01-01'
    ORDER BY customer_name
    """
    
    converter = SQLToSparkConverter()
    
    print("Original SQL:")
    print(sql_query)
    print("\n" + "=" * 50)
    
    # Test PySpark conversion
    print("PySpark DataFrame API:")
    print("-" * 30)
    try:
        pyspark_code = converter.to_pyspark(sql_query)
        print(pyspark_code)
    except Exception as e:
        print(f"Error in PySpark conversion: {e}")
    
    print("\n" + "=" * 50)
    
    # Test SparkSQL conversion
    print("SparkSQL:")
    print("-" * 30)
    try:
        sparksql_code = converter.to_sparksql(sql_query)
        print(sparksql_code)
    except Exception as e:
        print(f"Error in SparkSQL conversion: {e}")


def test_complex_query():
    """Test a complex SQL query with JOINs."""
    print("\n\nTesting Complex Query Conversion")
    print("=" * 50)
    
    sql_query = """
    SELECT c.customer_name, COUNT(o.order_id) as total_orders
    FROM customers c
    INNER JOIN orders o ON c.customer_id = o.customer_id
    WHERE o.order_date >= '2023-01-01'
    GROUP BY c.customer_name
    ORDER BY total_orders DESC
    """
    
    converter = SQLToSparkConverter()
    
    print("Original SQL:")
    print(sql_query)
    print("\n" + "=" * 50)
    
    # Test both conversions
    print("PySpark DataFrame API:")
    print("-" * 30)
    try:
        pyspark_code = converter.to_pyspark(sql_query)
        print(pyspark_code)
    except Exception as e:
        print(f"Error in PySpark conversion: {e}")
    
    print("\n" + "=" * 50)
    
    print("SparkSQL:")
    print("-" * 30)
    try:
        sparksql_code = converter.to_sparksql(sql_query)
        print(sparksql_code)
    except Exception as e:
        print(f"Error in SparkSQL conversion: {e}")


if __name__ == "__main__":
    print("SQL to Spark Converter - Test Suite")
    print("=" * 60)
    
    test_simple_query()
    test_complex_query()
    
    print("\n\nTest completed!")
