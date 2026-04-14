SELECT
  COUNT(*) AS weekend_payments
FROM dealership.payments_made
WHERE
  (
    (
      DAYOFWEEK(payment_date) + 5
    ) % 7
  ) IN (5, 6)
  AND vendor_name = 'Utility Company'
