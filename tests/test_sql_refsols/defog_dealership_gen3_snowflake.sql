WITH _T0 AS (
  SELECT
    SUM(payment_amount) AS AGG_0,
    payment_date AS PAYMENT_DATE,
    payment_method AS PAYMENT_METHOD
  FROM MAIN.PAYMENTS_RECEIVED
  WHERE
    DATEDIFF(WEEK, payment_date, CURRENT_TIMESTAMP()) = 1
  GROUP BY
    payment_date,
    payment_method
)
SELECT
  PAYMENT_DATE AS payment_date,
  PAYMENT_METHOD AS payment_method,
  COALESCE(AGG_0, 0) AS total_amount
FROM _T0
ORDER BY
  PAYMENT_DATE DESC NULLS LAST,
  PAYMENT_METHOD NULLS FIRST
