SELECT
  payment_date,
  payment_method,
  COALESCE(SUM(payment_amount), 0) AS total_amount
FROM defog.dealership.payments_received
WHERE
  CAST(DATEDIFF(
    DAY,
    DATE_ADD(CAST(payment_date AS DATE), -(
      (
        DAYOFWEEK(payment_date) + 5
      ) % 7
    )),
    DATE_ADD(
      CAST(CURRENT_TIMESTAMP() AS DATE),
      -(
        (
          DAYOFWEEK(CURRENT_TIMESTAMP()) + 5
        ) % 7
      )
    )
  ) / 7 AS BIGINT) = 1
GROUP BY
  1,
  2
ORDER BY
  1 DESC,
  2
