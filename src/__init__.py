"""
SQL to Spark Converter

A comprehensive tool for converting legacy SQL queries to Apache Spark code.
"""

from .parser.sql_parser import SQLParser
from .generators.pyspark_generator import PySparkGenerator
from .generators.sparksql_generator import SparkSQLGenerator

__version__ = "1.0.0"
__author__ = "Squad 06 - Hackathon Team"

class SQLToSparkConverter:
    """Main converter class that orchestrates SQL to Spark conversion."""
    
    def __init__(self):
        self.parser = SQLParser()
        self.pyspark_generator = PySparkGenerator()
        self.sparksql_generator = SparkSQLGenerator()
    
    def to_pyspark(self, sql_query: str) -> str:
        """Convert SQL query to PySpark DataFrame code."""
        parsed_query = self.parser.parse(sql_query)
        return self.pyspark_generator.generate(parsed_query)
    
    def to_sparksql(self, sql_query: str) -> str:
        """Convert SQL query to SparkSQL code."""
        parsed_query = self.parser.parse(sql_query)
        return self.sparksql_generator.generate(parsed_query)
    
    def convert_both(self, sql_query: str) -> dict:
        """Convert SQL query to both PySpark and SparkSQL."""
        parsed_query = self.parser.parse(sql_query)
        return {
            'pyspark': self.pyspark_generator.generate(parsed_query),
            'sparksql': self.sparksql_generator.generate(parsed_query)
        }

__all__ = ['SQLToSparkConverter', 'SQLParser', 'PySparkGenerator', 'SparkSQLGenerator']
