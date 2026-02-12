\set id random(1, 1000000)
\set total random(100, 50000)
\set status_code random(1, 4)

UPDATE orders
SET total_cents = :total,
    status = CASE
      WHEN :status_code = 1 THEN 'pending'
      WHEN :status_code = 2 THEN 'paid'
      WHEN :status_code = 3 THEN 'shipped'
      ELSE 'cancelled'
    END,
    created_at = clock_timestamp()
WHERE order_id = :id;
