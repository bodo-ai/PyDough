SELECT
  payment_date,
  payment_method,
  COALESCE(SUM(payment_amount), 0) AS total_amount
FROM MAIN.PAYMENTS_RECEIVED
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
          DAYOFWEEK(CURRENT_TIMESTAMP()) + 6
        ) % 7
      ) * -1,
      CURRENT_TIMESTAMP()
    )
  ) = 1
GROUP BY
  payment_date,
  payment_method
ORDER BY
  PAYMENT_DATE DESC NULLS LAST,
  PAYMENT_METHOD NULLS FIRST
