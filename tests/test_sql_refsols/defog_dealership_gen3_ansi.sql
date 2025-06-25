SELECT
  payment_date,
  payment_method,
  COALESCE(SUM(payment_amount), 0) AS total_amount
FROM main.payments_received
WHERE
  DATEDIFF(CURRENT_TIMESTAMP(), CAST(payment_date AS DATETIME), WEEK) = 1
GROUP BY
  payment_date,
  payment_method
ORDER BY
  payment_date DESC,
  payment_method
