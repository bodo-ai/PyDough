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
  1,
  2
ORDER BY
  1 DESC NULLS LAST,
  2 NULLS FIRST
