SELECT
  COUNT() AS weekend_payments
FROM main.payments_made
WHERE
  (
    (
      CAST(STRFTIME('%w', payment_date) AS INTEGER) + 6
    ) % 7
  ) IN (5, 6)
  AND vendor_name = 'Utility Company'
