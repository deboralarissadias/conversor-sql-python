"""
Enhanced SQL to Spark Converter

Improved converter that handles multiple queries and more SQL types.
"""

from typing import List, Dict, Any
from .parser.multi_query_parser import MultiQueryParser
from .generators.multi_generator import MultiPySparkGenerator, MultiSparkSQLGenerator


class EnhancedSQLToSparkConverter:
    """Enhanced converter with support for multiple queries and more SQL types."""
    
    def __init__(self):
        self.parser = MultiQueryParser()
        self.pyspark_generator = MultiPySparkGenerator()
        self.sparksql_generator = MultiSparkSQLGenerator()
    
    def convert_single(self, sql_query: str) -> Dict[str, str]:
        """Convert a single SQL query to both PySpark and SparkSQL."""
        try:
            parsed_query = self.parser.parse(sql_query)
            
            pyspark_code = self.pyspark_generator.generate(parsed_query)
            sparksql_code = self.sparksql_generator.generate(parsed_query)
            
            return {
                'pyspark': pyspark_code,
                'sparksql': sparksql_code,
                'query_type': parsed_query.query_type,
                'error': None
            }
        except Exception as e:
            return {
                'pyspark': f"# Error: {str(e)}",
                'sparksql': f"-- Error: {str(e)}",
                'query_type': 'ERROR',
                'error': str(e)
            }
    
    def convert_multiple(self, sql_text: str) -> Dict[str, Any]:
        """Convert multiple SQL queries to both PySpark and SparkSQL."""
        try:
            parsed_queries = self.parser.parse_multiple(sql_text)
            
            pyspark_code = self.pyspark_generator.generate_multiple(parsed_queries)
            sparksql_code = self.sparksql_generator.generate_multiple(parsed_queries)
            
            query_info = []
            for query in parsed_queries:
                query_info.append({
                    'type': query.query_type,
                    'original': query.original_query[:100] + '...' if len(query.original_query) > 100 else query.original_query,
                    'error': getattr(query, 'error_message', None)
                })
            
            return {
                'pyspark': pyspark_code,
                'sparksql': sparksql_code,
                'query_count': len(parsed_queries),
                'queries': query_info,
                'error': None
            }
        except Exception as e:
            return {
                'pyspark': f"# Error: {str(e)}",
                'sparksql': f"-- Error: {str(e)}",
                'query_count': 0,
                'queries': [],
                'error': str(e)
            }
    
    def to_pyspark(self, sql_query: str) -> str:
        """Convert SQL query to PySpark DataFrame code."""
        result = self.convert_single(sql_query)
        return result['pyspark']
    
    def to_sparksql(self, sql_query: str) -> str:
        """Convert SQL query to SparkSQL code."""
        result = self.convert_single(sql_query)
        return result['sparksql']
    
    def detect_multiple_queries(self, sql_text: str) -> bool:
        """Detect if the input contains multiple SQL queries."""
        import sqlparse
        statements = sqlparse.split(sql_text)
        valid_statements = [s.strip() for s in statements if s.strip() and not s.strip().startswith('--')]
        return len(valid_statements) > 1