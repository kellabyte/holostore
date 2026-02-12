\set id random(1000001, 2000000000)
\set customer random(2000, 2999)
\set total random(100, 50000)
\set status_code random(1, 4)

INSERT INTO orders (order_id, customer_id, status, total_cents, created_at)
VALUES (
  :id,
  :customer,
  CASE
    WHEN :status_code = 1 THEN 'pending'
    WHEN :status_code = 2 THEN 'paid'
    WHEN :status_code = 3 THEN 'shipped'
    ELSE 'cancelled'
  END,
  :total,
  clock_timestamp()
)
ON CONFLICT (order_id) DO UPDATE
SET customer_id = EXCLUDED.customer_id,
    status = EXCLUDED.status,
    total_cents = EXCLUDED.total_cents,
    created_at = EXCLUDED.created_at;
