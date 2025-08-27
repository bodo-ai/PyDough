SELECT
  COUNT(*) AS weekend_payments
FROM main.payments_made
WHERE
  (
    (
      EXTRACT(DOW FROM CAST(payment_date AS TIMESTAMP)) + 6
    ) % 7
  ) IN (5, 6)
  AND vendor_name = 'Utility Company'
