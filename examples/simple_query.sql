-- Simple SELECT query example
SELECT 
    customer_id,
    customer_name,
    email,
    registration_date
FROM customers
WHERE registration_date >= '2023-01-01'
ORDER BY customer_name;
