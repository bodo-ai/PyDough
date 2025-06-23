WITH _t0 AS (
  SELECT
    COALESCE(SUM(payment_amount), 0) AS total_amount,
    COUNT(*) AS n_rows,
    payment_method
  FROM main.payments_received
  GROUP BY
    payment_method
)
SELECT
  payment_method,
  n_rows AS total_payments,
  total_amount
FROM _t0
ORDER BY
  total_amount DESC
LIMIT 3
