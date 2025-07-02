-- Complex query with JOINs and aggregations
SELECT 
    c.customer_name,
    c.email,
    COUNT(o.order_id) as total_orders,
    SUM(o.total_amount) as total_spent,
    AVG(o.total_amount) as avg_order_value,
    MAX(o.order_date) as last_order_date
FROM customers c
INNER JOIN orders o ON c.customer_id = o.customer_id
LEFT JOIN customer_segments cs ON c.customer_id = cs.customer_id
WHERE o.order_date >= '2023-01-01'
  AND o.status = 'completed'
  AND c.active = 1
GROUP BY c.customer_id, c.customer_name, c.email
HAVING COUNT(o.order_id) >= 3
ORDER BY total_spent DESC, customer_name ASC;
