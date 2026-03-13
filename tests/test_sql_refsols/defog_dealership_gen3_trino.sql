SELECT
  payment_date,
  payment_method,
  COALESCE(SUM(payment_amount), 0) AS total_amount
FROM main.payments_received
WHERE
  DATE_DIFF('WEEK', CAST(payment_date AS TIMESTAMP), CURRENT_TIMESTAMP) = 1
GROUP BY
  1,
  2
ORDER BY
  1 DESC,
  2 NULLS FIRST
