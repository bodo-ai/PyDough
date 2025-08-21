SELECT
  payment_method,
  COUNT(*) AS total_payments,
  COALESCE(SUM(payment_amount), 0) AS total_amount
FROM MAIN.PAYMENTS_RECEIVED
GROUP BY
  1
ORDER BY
  3 DESC NULLS LAST
LIMIT 3
