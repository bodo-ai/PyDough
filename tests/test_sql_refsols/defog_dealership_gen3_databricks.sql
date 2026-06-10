SELECT
  payment_date,
  payment_method,
  COALESCE(SUM(payment_amount), 0) AS total_amount
FROM main.payments_received
WHERE
  CAST(DATEDIFF(
    DAY,
    DATEADD(
      DAY,
      -(
        (
          DAYOFWEEK(TO_DATE(payment_date)) - 1 + 6
        ) % 7
      ),
      CAST(payment_date AS DATE)
    ),
    DATEADD(
      DAY,
      -(
        (
          DAYOFWEEK(TO_DATE(CURRENT_TIMESTAMP())) - 1 + 6
        ) % 7
      ),
      CAST(CURRENT_TIMESTAMP() AS DATE)
    )
  ) / 7 AS BIGINT) = 1
GROUP BY
  1,
  2
ORDER BY
  1 DESC,
  2
