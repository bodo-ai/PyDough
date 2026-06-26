SELECT
  CAST(CAST(payments_received.payment_date AS TIMESTAMP) AS DATE) - CAST((
    (
      DAYOFWEEK(CAST(payments_received.payment_date AS TIMESTAMP)) + 6
    ) % 7
  ) AS INT) AS payment_week,
  COUNT(*) AS total_payments,
  COALESCE(
    COUNT_IF((
      (
        DAYOFWEEK(payments_received.payment_date) + 6
      ) % 7
    ) IN (5, 6)),
    0
  ) AS weekend_payments
FROM main.payments_received AS payments_received
JOIN main.sales AS sales
  ON payments_received.sale_id = sales._id AND sales.sale_price > 30000
WHERE
  CAST(DATE_DIFF(
    'DAY',
    CAST(payments_received.payment_date AS DATE) - CAST((
      (
        DAYOFWEEK(payments_received.payment_date) + 6
      ) % 7
    ) AS INT),
    CAST(CAST(CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS TIMESTAMP) AS DATE) - CAST((
      (
        DAYOFWEEK(CAST(CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS TIMESTAMP)) + 6
      ) % 7
    ) AS INT)
  ) / 7 AS BIGINT) <= 8
  AND CAST(DATE_DIFF(
    'DAY',
    CAST(payments_received.payment_date AS DATE) - CAST((
      (
        DAYOFWEEK(payments_received.payment_date) + 6
      ) % 7
    ) AS INT),
    CAST(CAST(CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS TIMESTAMP) AS DATE) - CAST((
      (
        DAYOFWEEK(CAST(CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS TIMESTAMP)) + 6
      ) % 7
    ) AS INT)
  ) / 7 AS BIGINT) >= 1
GROUP BY
  1
