WITH _t0 AS (
  SELECT
    SUM(payment_amount) AS agg_0,
    payment_date,
    payment_method
  FROM main.payments_received
  WHERE
    DATEDIFF(CURRENT_TIMESTAMP(), CAST(payment_date AS DATETIME), WEEK) = 1
  GROUP BY
    payment_date,
    payment_method
)
SELECT
  payment_date,
  payment_method,
  COALESCE(agg_0, 0) AS total_amount
FROM _t0
ORDER BY
  payment_date DESC,
  payment_method
