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
FROM postgres.main.payments_received AS payments_received
JOIN postgres.main.sales AS sales
  ON payments_received.sale_id = sales._id AND sales.sale_price > 30000
WHERE
  DATE_DIFF(
    'WEEK',
    CAST(DATE_TRUNC(
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
    ) AS TIMESTAMP),
    CAST(DATE_TRUNC(
      'DAY',
      DATE_ADD(
        'DAY',
        (
          (
            DAY_OF_WEEK(CURRENT_TIMESTAMP) - 1
          ) % 7
        ) * -1,
        CURRENT_TIMESTAMP
      )
    ) AS TIMESTAMP)
  ) <= 8
  AND DATE_DIFF(
    'WEEK',
    CAST(DATE_TRUNC(
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
    ) AS TIMESTAMP),
    CAST(DATE_TRUNC(
      'DAY',
      DATE_ADD(
        'DAY',
        (
          (
            DAY_OF_WEEK(CURRENT_TIMESTAMP) - 1
          ) % 7
        ) * -1,
        CURRENT_TIMESTAMP
      )
    ) AS TIMESTAMP)
  ) >= 1
GROUP BY
  1
