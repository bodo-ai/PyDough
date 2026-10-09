SELECT
  COUNT(*) AS weekend_payments
FROM MAIN.PAYMENTS_MADE
WHERE
  (
    MOD((
      TO_CHAR(PAYMENT_DATE, 'D') + 5
    ), 7)
  ) IN (5, 6)
  AND VENDOR_NAME = 'Utility Company'
