SELECT
  DATE_TRUNC(
    'DAY',
    DATE_ADD(
      'DAY',
      (
        (
          (
            (
              DAY_OF_WEEK(CAST(payments_received.payment_date AS TIMESTAMP)) % 7
            ) + 1
          ) + -1
        ) % 7
      ) * -1,
      CAST(payments_received.payment_date AS TIMESTAMP)
    )
  ) AS payment_week,
  COUNT(*) AS total_payments,
  COALESCE(
    SUM(
      (
        (
          (
            DAY_OF_WEEK(payments_received.payment_date) % 7
          ) + 0
        ) % 7
      ) IN (5, 6)
    ),
    0
  ) AS weekend_payments
FROM main.payments_received AS payments_received
JOIN main.sales AS sales
  ON payments_received.sale_id = sales._id AND sales.sale_price > 30000
WHERE
  DATE_DIFF('WEEK', CAST(payments_received.payment_date AS TIMESTAMP), CURRENT_TIMESTAMP) <= 8
  AND DATE_DIFF('WEEK', CAST(payments_received.payment_date AS TIMESTAMP), CURRENT_TIMESTAMP) >= 1
GROUP BY
  1
