SELECT
  payment_date,
  payment_method,
  COALESCE(SUM(payment_amount), 0) AS total_amount
FROM main.payments_received
WHERE
  CAST((
    DATEDIFF(CURRENT_TIMESTAMP(), CAST(payment_date AS DATETIME)) + (
      (
        DAYOFWEEK(payment_date) + 5
      ) % 7
    ) - (
      (
        DAYOFWEEK(CURRENT_TIMESTAMP()) + 5
      ) % 7
    )
  ) / 7 AS SIGNED) = 1
GROUP BY
  1,
  2
ORDER BY
  payment_date DESC,
  payment_method COLLATE utf8mb4_bin
