SELECT
  COUNT(*) AS weekend_payments
FROM MAIN.PAYMENTS_MADE
WHERE
  (
    (
      DAYOFWEEK(payment_date) + 6
    ) % 7
  ) IN (5, 6)
  AND vendor_name = 'Utility Company'
