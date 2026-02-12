\set id random(1, 1000000)

SELECT order_id, customer_id, status, total_cents, created_at
FROM orders
WHERE order_id = :id;
