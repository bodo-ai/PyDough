SELECT
  payment_date,
  payment_method,
  COALESCE(SUM(payment_amount), 0) AS total_amount
FROM main.payments_received
WHERE
  CAST(CAST((
    CAST(CURRENT_TIMESTAMP AS DATE) - CAST(payment_date AS DATE) + (
      (
        EXTRACT(DOW FROM CAST(payment_date AS TIMESTAMP)) + 6
      ) % 7
    ) - (
      (
        EXTRACT(DOW FROM CURRENT_TIMESTAMP) + 6
      ) % 7
    )
  ) AS DOUBLE PRECISION) / 7 AS BIGINT) = 1
GROUP BY
  1,
  2
ORDER BY
  1 DESC NULLS LAST,
  2 NULLS FIRST
