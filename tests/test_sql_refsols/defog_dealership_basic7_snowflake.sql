WITH _T1 AS (
  SELECT
    SUM(payment_amount) AS AGG_0,
    COUNT(*) AS AGG_1,
    payment_method AS PAYMENT_METHOD
  FROM MAIN.PAYMENTS_RECEIVED
  GROUP BY
    payment_method
)
SELECT
  PAYMENT_METHOD AS payment_method,
  AGG_1 AS total_payments,
  COALESCE(AGG_0, 0) AS total_amount
FROM _T1
ORDER BY
  TOTAL_AMOUNT DESC NULLS LAST
LIMIT 3
