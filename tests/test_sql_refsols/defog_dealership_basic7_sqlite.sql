SELECT
  payment_method,
  COUNT(*) AS total_payments,
  COALESCE(SUM(payment_amount), 0) AS total_amount
FROM main.payments_received
GROUP BY
  payment_method
ORDER BY
  total_amount DESC
LIMIT 3
