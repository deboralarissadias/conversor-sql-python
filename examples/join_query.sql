-- Multiple JOINs query
SELECT 
    u.username,
    u.email,
    p.title as profile_title,
    c.company_name,
    COUNT(o.order_id) as total_orders
FROM users u
LEFT JOIN profiles p ON u.user_id = p.user_id
INNER JOIN companies c ON u.company_id = c.company_id
LEFT JOIN orders o ON u.user_id = o.customer_id
WHERE u.active = 1 
  AND c.status = 'verified'
GROUP BY u.user_id, u.username, u.email, p.title, c.company_name
ORDER BY total_orders DESC;
