"""
Result Validator

Validates the results of SQL to Spark conversion.
"""

from typing import Dict, List, Any, Optional


class ResultValidator:
    """Validates the results of SQL to Spark conversion."""
    
    def __init__(self):
        self.validation_rules = {
            'pyspark': self._validate_pyspark_code,
            'sparksql': self._validate_sparksql_code
        }
    
    def validate(self, original_sql: str, converted_code: str, 
                 format_type: str) -> Dict[str, Any]:
        """
        Validate the converted code against the original SQL.
        
        Args:
            original_sql: Original SQL query
            converted_code: Converted PySpark or SparkSQL code
            format_type: Type of conversion ('pyspark' or 'sparksql')
            
        Returns:
            Dictionary with validation results
        """
        if format_type not in self.validation_rules:
            return {
                'valid': False,
                'errors': [f"Unknown format type: {format_type}"]
            }
        
        # Run format-specific validation
        validator_func = self.validation_rules[format_type]
        return validator_func(original_sql, converted_code)
    
    def _validate_pyspark_code(self, original_sql: str, 
                              pyspark_code: str) -> Dict[str, Any]:
        """Validate PySpark DataFrame API code."""
        errors = []
        warnings = []
        
        # Check for basic PySpark structure
        if "spark = SparkSession" not in pyspark_code:
            errors.append("Missing SparkSession initialization")
        
        if "df = " not in pyspark_code:
            errors.append("No DataFrame operations found")
        
        # Check for common issues
        if "df.show()" not in pyspark_code:
            warnings.append("Missing df.show() to display results")
        
        # Check for imports
        if "from pyspark.sql import SparkSession" not in pyspark_code:
            errors.append("Missing required import: SparkSession")
        
        if "from pyspark.sql.functions" not in pyspark_code:
            warnings.append("Missing recommended import: pyspark.sql.functions")
        
        return {
            'valid': len(errors) == 0,
            'errors': errors,
            'warnings': warnings
        }
    
    def _validate_sparksql_code(self, original_sql: str, 
                               sparksql_code: str) -> Dict[str, Any]:
        """Validate SparkSQL code."""
        errors = []
        warnings = []
        
        # Check for basic SparkSQL structure
        if "spark.sql" not in sparksql_code:
            errors.append("Missing spark.sql() execution")
        
        # Check for triple quotes
        if '"""' not in sparksql_code:
            warnings.append("Missing triple quotes for SQL string")
        
        # Check for semicolon at the end of SQL
        if not sparksql_code.strip().endswith('"""") and \
           not sparksql_code.strip().endswith('""");'):
            warnings.append("SQL query should end with a semicolon")
        
        return {
            'valid': len(errors) == 0,
            'errors': errors,
            'warnings': warnings
        }