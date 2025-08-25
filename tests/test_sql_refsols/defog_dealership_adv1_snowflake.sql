SELECT
  DATE_TRUNC(
    'DAY',
    DATEADD(
      DAY,
      (
        (
          DAYOFWEEK(CAST(PAYMENTS_RECEIVED.payment_date AS TIMESTAMP)) + 6
        ) % 7
      ) * -1,
      CAST(PAYMENTS_RECEIVED.payment_date AS TIMESTAMP)
    )
  ) AS payment_week,
  COUNT(*) AS total_payments,
  COALESCE(
    COUNT_IF((
      (
        DAYOFWEEK(PAYMENTS_RECEIVED.payment_date) + 6
      ) % 7
    ) IN (5, 6)),
    0
  ) AS weekend_payments
FROM MAIN.PAYMENTS_RECEIVED AS PAYMENTS_RECEIVED
JOIN MAIN.SALES AS SALES
  ON PAYMENTS_RECEIVED.sale_id = SALES._id AND SALES.sale_price > 30000
WHERE
  DATEDIFF(
    WEEK,
    CAST(DATEADD(
      DAY,
      (
        (
          DAYOFWEEK(PAYMENTS_RECEIVED.payment_date) + 6
        ) % 7
      ) * -1,
      PAYMENTS_RECEIVED.payment_date
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
          DAYOFWEEK(PAYMENTS_RECEIVED.payment_date) + 6
        ) % 7
      ) * -1,
      PAYMENTS_RECEIVED.payment_date
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
