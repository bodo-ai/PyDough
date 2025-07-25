SELECT
  payment_date,
  payment_method,
  COALESCE(SUM(payment_amount), 0) AS total_amount
FROM main.payments_received
WHERE
  EXTRACT(DAYS FROM (
    CURRENT_TIMESTAMP - CAST(payment_date AS TIMESTAMP)
  )) / 7 = 1
GROUP BY
  payment_date,
  payment_method
ORDER BY
  payment_date DESC NULLS LAST,
  payment_method NULLS FIRST
