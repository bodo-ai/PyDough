WITH _t2 AS (
  SELECT
    COUNT() AS agg_1,
    SUM(payment_amount) AS agg_0,
    payment_method
  FROM main.payments_received
  GROUP BY
    payment_method
), _t0 AS (
  SELECT
    COALESCE(agg_0, 0) AS ordering_2,
    payment_method,
    COALESCE(agg_0, 0) AS total_amount,
    COALESCE(agg_1, 0) AS total_payments
  FROM _t2
  ORDER BY
    ordering_2 DESC
  LIMIT 3
)
SELECT
  payment_method,
  total_payments,
  total_amount
FROM _t0
ORDER BY
  ordering_2 DESC
