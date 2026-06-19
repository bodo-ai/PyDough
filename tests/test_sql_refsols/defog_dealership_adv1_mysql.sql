SELECT
  CAST(DATE_SUB(
    CAST(payments_received.payment_date AS DATETIME),
    INTERVAL (
      (
        DAYOFWEEK(CAST(payments_received.payment_date AS DATETIME)) + 5
      ) % 7
    ) DAY
  ) AS DATE) AS payment_week,
  COUNT(*) AS total_payments,
  COALESCE(
    SUM((
      (
        DAYOFWEEK(payments_received.payment_date) + 5
      ) % 7
    ) IN (5, 6)),
    0
  ) AS weekend_payments
FROM dealership.payments_received AS payments_received
JOIN dealership.sales AS sales
  ON payments_received.sale_id = sales._id AND sales.sale_price > 30000
WHERE
  CAST((
    DATEDIFF(CURRENT_TIMESTAMP(), CAST(payments_received.payment_date AS DATETIME)) + (
      (
        DAYOFWEEK(payments_received.payment_date) + 5
      ) % 7
    ) - (
      (
        DAYOFWEEK(CURRENT_TIMESTAMP()) + 5
      ) % 7
    )
  ) / 7 AS SIGNED) <= 8
  AND CAST((
    DATEDIFF(CURRENT_TIMESTAMP(), CAST(payments_received.payment_date AS DATETIME)) + (
      (
        DAYOFWEEK(payments_received.payment_date) + 5
      ) % 7
    ) - (
      (
        DAYOFWEEK(CURRENT_TIMESTAMP()) + 5
      ) % 7
    )
  ) / 7 AS SIGNED) >= 1
GROUP BY
  1
