SELECT
  payment_date,
  payment_method,
  COALESCE(SUM(payment_amount), 0) AS total_amount
FROM mongo.defog.payments_received
WHERE
  CAST(CAST((
    DATE_DIFF(
      'DAY',
      CAST(DATE_TRUNC('DAY', CAST(payment_date AS TIMESTAMP)) AS TIMESTAMP),
      CAST(DATE_TRUNC('DAY', CURRENT_TIMESTAMP) AS TIMESTAMP)
    ) + (
      (
        DAY_OF_WEEK(CAST(payment_date AS TIMESTAMP)) - 1
      ) % 7
    ) - (
      (
        DAY_OF_WEEK(CURRENT_TIMESTAMP) - 1
      ) % 7
    )
  ) AS DOUBLE) / 7 AS BIGINT) = 1
GROUP BY
  1,
  2
ORDER BY
  1 DESC,
  2 NULLS FIRST
