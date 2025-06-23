WITH _t1 AS (
  SELECT
    COUNT(*) AS n_rows,
    SUM(payment_amount) AS sum_payment_amount,
    payment_method
  FROM main.payments_received
  GROUP BY
    payment_method
)
SELECT
  payment_method,
  n_rows AS total_payments,
  COALESCE(sum_payment_amount, 0) AS total_amount
FROM _t1
ORDER BY
  total_amount DESC
LIMIT 3
