#!/usr/bin/env python3
"""
SQL to Spark Converter - Main CLI Interface

A tool to convert Oracle/PostgreSQL SQL queries to Apache Spark code.
"""

import argparse
import sys
from pathlib import Path
from src import SQLToSparkConverter


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Convert SQL queries to Apache Spark code (PySpark DataFrame API and SparkSQL)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --input query.sql --output pyspark
  %(prog)s --query "SELECT * FROM users WHERE age > 25" --output sparksql
  %(prog)s --input query.sql --output both --output-file output.py
        """
    )
    
    # Input options
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument(
        '-f', '--input', '--file',
        type=str,
        help='Input SQL file path'
    )
    input_group.add_argument(
        '-q', '--query',
        type=str,
        help='SQL query string'
    )
    
    # Output format
    parser.add_argument(
        '--output',
        choices=['pyspark', 'sparksql', 'both'],
        default='both',
        help='Output format (default: both)'
    )
    
    # Output file
    parser.add_argument(
        '-o', '--output-file',
        type=str,
        help='Output file path (default: stdout)'
    )
    
    # Input directory for batch processing
    parser.add_argument(
        '--input-dir',
        type=str,
        help='Input directory containing SQL files'
    )
    
    # Output directory for batch results
    parser.add_argument(
        '--output-dir',
        type=str,
        help='Output directory for converted files'
    )
    
    # Validation mode
    parser.add_argument(
        '--validate',
        action='store_true',
        help='Validate the SQL query without conversion'
    )
    
    # Verbose mode
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Enable verbose output'
    )
    
    # Version
    parser.add_argument(
        '--version',
        action='version',
        version='SQL to Spark Converter 1.0.0'
    )
    
    args = parser.parse_args()
    
    try:
        # Initialize converter
        converter = SQLToSparkConverter()
        
        # Get SQL query
        if args.input:
            sql_file = Path(args.input)
            if not sql_file.exists():
                print(f"Error: File '{args.input}' not found.", file=sys.stderr)
                sys.exit(1)
            
            with open(sql_file, 'r', encoding='utf-8') as f:
                sql_query = f.read().strip()
            
            if args.verbose:
                print(f"Reading SQL from: {args.input}")
        else:
            sql_query = args.query.strip()
        
        if not sql_query:
            print("Error: Empty SQL query provided.", file=sys.stderr)
            sys.exit(1)
        
        if args.verbose:
            print(f"Processing SQL query...")
            print(f"Output format: {args.output}")
        
        # Generate output based on format
        output_lines = []
        
        if args.output in ['pyspark', 'both']:
            if args.verbose:
                print("Generating PySpark DataFrame API code...")
            
            pyspark_code = converter.to_pyspark(sql_query)
            output_lines.append("# " + "="*60)
            output_lines.append("# PySpark DataFrame API Code")
            output_lines.append("# " + "="*60)
            output_lines.append("")
            output_lines.append(pyspark_code)
            output_lines.append("")
        
        if args.output in ['sparksql', 'both']:
            if args.verbose:
                print("Generating SparkSQL code...")
            
            sparksql_code = converter.to_sparksql(sql_query)
            output_lines.append("# " + "="*60)
            output_lines.append("# SparkSQL Code")
            output_lines.append("# " + "="*60)
            output_lines.append("")
            output_lines.append(sparksql_code)
            output_lines.append("")
        
        # Output results
        final_output = "\n".join(output_lines)
        
        if args.output_file:
            output_path = Path(args.output_file)
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(final_output)
            
            if args.verbose:
                print(f"Output written to: {args.output_file}")
        else:
            print(final_output)
        
        if args.verbose:
            print("Conversion completed successfully!")
    
    except Exception as e:
        print(f"Error: {str(e)}", file=sys.stderr)
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()