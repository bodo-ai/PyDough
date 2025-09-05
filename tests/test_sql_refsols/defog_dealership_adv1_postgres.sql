SELECT
  DATE_TRUNC(
    'DAY',
    CAST(payments_received.payment_date AS TIMESTAMP) - MAKE_INTERVAL(
      days => (
        EXTRACT(DOW FROM CAST(payments_received.payment_date AS TIMESTAMP)) + 6
      ) % 7
    )
  ) AS payment_week,
  COUNT(*) AS total_payments,
  COALESCE(
    SUM(
      CASE
        WHEN (
          (
            EXTRACT(DOW FROM CAST(payments_received.payment_date AS TIMESTAMP)) + 6
          ) % 7
        ) IN (5, 6)
        THEN 1
        ELSE 0
      END
    ),
    0
  ) AS weekend_payments
FROM main.payments_received AS payments_received
JOIN main.sales AS sales
  ON payments_received.sale_id = sales._id AND sales.sale_price > 30000
WHERE
  CAST(CAST(EXTRACT(EPOCH FROM (
    CURRENT_TIMESTAMP - payments_received.payment_date
  )) AS DOUBLE PRECISION) / 604800 AS BIGINT) <= 8
  AND CAST(CAST(EXTRACT(EPOCH FROM (
    CURRENT_TIMESTAMP - payments_received.payment_date
  )) AS DOUBLE PRECISION) / 604800 AS BIGINT) >= 1
GROUP BY
  1
