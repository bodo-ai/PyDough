SELECT
  DATE_TRUNC(
    'DAY',
    DATE_ADD(
      'DAY',
      (
        (
          DAY_OF_WEEK(CAST(payments_received.payment_date AS TIMESTAMP)) - 1
        ) % 7
      ) * -1,
      CAST(payments_received.payment_date AS TIMESTAMP)
    )
  ) AS payment_week,
  COUNT(*) AS total_payments,
  COUNT_IF((
    (
      DAY_OF_WEEK(payments_received.payment_date) - 1
    ) % 7
  ) IN (5, 6)) AS weekend_payments
FROM mongo.defog.payments_received AS payments_received
JOIN cassandra.defog.sales AS sales
  ON payments_received.sale_id = sales._id AND sales.sale_price > 30000
WHERE
  CAST(CAST((
    DATE_DIFF(
      'DAY',
      CAST(DATE_TRUNC('DAY', CAST(payments_received.payment_date AS TIMESTAMP)) AS TIMESTAMP),
      CAST(DATE_TRUNC('DAY', CURRENT_TIMESTAMP) AS TIMESTAMP)
    ) + (
      (
        DAY_OF_WEEK(CAST(payments_received.payment_date AS TIMESTAMP)) - 1
      ) % 7
    ) - (
      (
        DAY_OF_WEEK(CURRENT_TIMESTAMP) - 1
      ) % 7
    )
  ) AS DOUBLE) / 7 AS BIGINT) <= 8
  AND CAST(CAST((
    DATE_DIFF(
      'DAY',
      CAST(DATE_TRUNC('DAY', CAST(payments_received.payment_date AS TIMESTAMP)) AS TIMESTAMP),
      CAST(DATE_TRUNC('DAY', CURRENT_TIMESTAMP) AS TIMESTAMP)
    ) + (
      (
        DAY_OF_WEEK(CAST(payments_received.payment_date AS TIMESTAMP)) - 1
      ) % 7
    ) - (
      (
        DAY_OF_WEEK(CURRENT_TIMESTAMP) - 1
      ) % 7
    )
  ) AS DOUBLE) / 7 AS BIGINT) >= 1
GROUP BY
  1
