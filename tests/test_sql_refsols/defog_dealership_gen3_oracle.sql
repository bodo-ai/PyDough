SELECT
  payment_date,
  payment_method,
  NVL(SUM(payment_amount), 0) AS total_amount
FROM MAIN.PAYMENTS_RECEIVED
WHERE
  DATEDIFF(CURRENT_TIMESTAMP, CAST(payment_date AS DATETIME), WEEK) = 1
GROUP BY
  payment_date,
  payment_method
ORDER BY
  1 DESC NULLS LAST,
  2 NULLS FIRST
