SELECT
  DATE_TRUNC(
    'DAY',
    DATEADD(
      DAY,
      (
        (
          DAYOFWEEK(CAST(payments_received.payment_date AS TIMESTAMP)) + 6
        ) % 7
      ) * -1,
      CAST(payments_received.payment_date AS TIMESTAMP)
    )
  ) AS payment_week,
  COUNT(*) AS total_payments,
  COUNT_IF((
    (
      DAYOFWEEK(payments_received.payment_date) + 6
    ) % 7
  ) IN (5, 6)) AS weekend_payments
FROM main.payments_received AS payments_received
JOIN main.sales AS sales
  ON payments_received.sale_id = sales._id AND sales.sale_price > 30000
WHERE
  DATEDIFF(
    WEEK,
    CAST(DATEADD(
      DAY,
      (
        (
          DAYOFWEEK(payments_received.payment_date) + 6
        ) % 7
      ) * -1,
      payments_received.payment_date
    ) AS DATETIME),
    DATEADD(
      DAY,
      (
        (
          DAYOFWEEK(CURRENT_TIMESTAMP()) + 6
        ) % 7
      ) * -1,
      CURRENT_TIMESTAMP()
    )
  ) <= 8
  AND DATEDIFF(
    WEEK,
    CAST(DATEADD(
      DAY,
      (
        (
          DAYOFWEEK(payments_received.payment_date) + 6
        ) % 7
      ) * -1,
      payments_received.payment_date
    ) AS DATETIME),
    DATEADD(
      DAY,
      (
        (
          DAYOFWEEK(CURRENT_TIMESTAMP()) + 6
        ) % 7
      ) * -1,
      CURRENT_TIMESTAMP()
    )
  ) >= 1
GROUP BY
  1
