SELECT
  payment_date,
  payment_method,
  COALESCE(SUM(payment_amount), 0) AS total_amount
FROM main.payments_received
WHERE
  CAST(DATE_DIFF(
    'DAY',
    CAST(payment_date AS DATE) - CAST((
      (
        DAYOFWEEK(payment_date) + 6
      ) % 7
    ) AS INT),
    CAST(CAST(CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS TIMESTAMP) AS DATE) - CAST((
      (
        DAYOFWEEK(CAST(CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS TIMESTAMP)) + 6
      ) % 7
    ) AS INT)
  ) / 7 AS BIGINT) = 1
GROUP BY
  1,
  2
ORDER BY
  1 DESC,
  2 NULLS FIRST
