SELECT
  COUNT() AS weekend_payments
FROM main.payments_made
WHERE
  (
    (
      DAY_OF_WEEK(payment_date) + 6
    ) % 7
  ) IN (5, 6)
  AND vendor_name = 'Utility Company'
