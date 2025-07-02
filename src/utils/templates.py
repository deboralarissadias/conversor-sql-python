"""
Templates for code generation
"""

PYSPARK_IMPORTS = """from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
"""

SPARK_SESSION_INIT = """# Initialize Spark Session
spark = SparkSession.builder.appName("SQL_to_Spark_Converter").getOrCreate()
"""

PYSPARK_SHOW_RESULTS = """# Show results
df.show()
"""

SPARKSQL_HEADER = """# Generated SparkSQL Code
# Execute this code in a Spark environment
"""

SPARKSQL_WRAPPER_START = """spark.sql(\"\"\"
"""

SPARKSQL_WRAPPER_END = """
\"\"\")
"""