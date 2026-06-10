SELECT
  COUNT(*) AS weekend_payments
FROM main.payments_made
WHERE
  (
    (
      DAYOFWEEK(TO_DATE(payment_date)) - 1 + 6
    ) % 7
  ) IN (5, 6)
  AND vendor_name = 'Utility Company'
