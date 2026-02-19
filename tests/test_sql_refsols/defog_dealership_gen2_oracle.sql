SELECT
  COUNT(*) AS weekend_payments
FROM MAIN.PAYMENTS_MADE
WHERE
  (
    MOD((
      TO_CHAR(payment_date, 'D') + 5
    ), 7)
  ) IN (5, 6)
  AND vendor_name = 'Utility Company'
