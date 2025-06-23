WITH _t0 AS (
  SELECT
    SUM(payment_amount) AS sum_payment_amount,
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
  COALESCE(sum_payment_amount, 0) AS total_amount
FROM _t0
ORDER BY
  payment_date DESC,
  payment_method
