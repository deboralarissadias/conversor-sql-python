#!/usr/bin/env python3
"""
Test script for the enhanced SQL to Spark converter
"""

from src.enhanced_converter import EnhancedSQLToSparkConverter

def test_single_query():
    """Test single query conversion."""
    print("=== Teste de Query Única ===")
    converter = EnhancedSQLToSparkConverter()
    
    sql = "SELECT name, age FROM users WHERE age > 25 ORDER BY name"
    result = converter.convert_single(sql)
    
    print(f"Tipo de Query: {result['query_type']}")
    print(f"PySpark:\n{result['pyspark']}")
    print(f"SparkSQL:\n{result['sparksql']}")
    print()

def test_multiple_queries():
    """Test multiple queries conversion."""
    print("=== Teste de Múltiplas Queries ===")
    converter = EnhancedSQLToSparkConverter()
    
    sql = """
    -- Criar tabela
    CREATE TABLE users (
        id INT PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        age INT
    );
    
    -- Inserir dados
    INSERT INTO users (id, name, age) VALUES (1, 'João', 30);
    INSERT INTO users (id, name, age) VALUES (2, 'Maria', 25);
    
    -- Consultar dados
    SELECT name, age FROM users WHERE age > 25;
    
    -- Atualizar dados
    UPDATE users SET age = 31 WHERE id = 1;
    
    -- Deletar dados
    DELETE FROM users WHERE age < 26;
    """
    
    result = converter.convert_multiple(sql)
    
    print(f"Número de Queries: {result['query_count']}")
    print("Queries encontradas:")
    for i, query in enumerate(result['queries'], 1):
        print(f"  {i}. {query['type']}: {query['original'][:50]}...")
    
    print(f"\nPySpark:\n{result['pyspark']}")
    print(f"\nSparkSQL:\n{result['sparksql']}")
    print()

def test_complex_query():
    """Test complex query with JOINs."""
    print("=== Teste de Query Complexa ===")
    converter = EnhancedSQLToSparkConverter()
    
    sql = """
    SELECT 
        c.customer_name,
        c.email,
        COUNT(o.order_id) as total_orders,
        SUM(o.total_amount) as total_spent
    FROM customers c
    INNER JOIN orders o ON c.customer_id = o.customer_id
    WHERE o.order_date >= '2023-01-01'
    GROUP BY c.customer_id, c.customer_name, c.email
    HAVING COUNT(o.order_id) >= 3
    ORDER BY total_spent DESC
    """
    
    result = converter.convert_single(sql)
    
    print(f"Tipo de Query: {result['query_type']}")
    print(f"PySpark:\n{result['pyspark']}")
    print(f"SparkSQL:\n{result['sparksql']}")
    print()

def test_error_handling():
    """Test error handling."""
    print("=== Teste de Tratamento de Erros ===")
    converter = EnhancedSQLToSparkConverter()
    
    # Query com erro de sintaxe
    sql = "SELECT * FROM WHERE age > 25"
    result = converter.convert_single(sql)
    
    print(f"Tipo de Query: {result['query_type']}")
    print(f"Erro: {result['error']}")
    print()

if __name__ == "__main__":
    print("Testando o Conversor SQL to Spark Aprimorado\n")
    
    test_single_query()
    test_multiple_queries()
    test_complex_query()
    test_error_handling()
    
    print("Todos os testes concluídos!")
