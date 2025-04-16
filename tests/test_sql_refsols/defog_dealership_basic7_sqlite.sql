WITH _t1 AS (
  SELECT
    COUNT() AS agg_1,
    SUM(payment_amount) AS agg_0,
    payment_method
  FROM main.payments_received
  GROUP BY
    payment_method
)
SELECT
  payment_method,
  COALESCE(agg_1, 0) AS total_payments,
  COALESCE(agg_0, 0) AS total_amount
FROM _t1
ORDER BY
  total_amount DESC
LIMIT 3
