SELECT
  payment_date,
  payment_method,
  COALESCE(SUM(payment_amount), 0) AS total_amount
FROM dealership.payments_received
WHERE
  DATEDIFF(
    WEEK,
    CAST(DATEADD(DAY, (
      (
        DAYOFWEEK(payment_date) + 6
      ) % 7
    ) * -1, payment_date) AS DATETIME),
    DATEADD(
      DAY,
      (
        (
          DAYOFWEEK(CAST(CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()) AS TIMESTAMPNTZ)) + 6
        ) % 7
      ) * -1,
      CAST(CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()) AS TIMESTAMPNTZ)
    )
  ) = 1
GROUP BY
  1,
  2
ORDER BY
  1 DESC NULLS LAST,
  2 NULLS FIRST
