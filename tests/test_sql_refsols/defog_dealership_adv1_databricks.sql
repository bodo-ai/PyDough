SELECT
  DATEADD(
    DAY,
    -(
      (
        DAYOFWEEK(TO_DATE(CAST(payments_received.payment_date AS TIMESTAMP))) + 5
      ) % 7
    ),
    CAST(CAST(payments_received.payment_date AS TIMESTAMP) AS DATE)
  ) AS payment_week,
  COUNT(*) AS total_payments,
  COUNT_IF(
    (
      (
        DAYOFWEEK(TO_DATE(payments_received.payment_date)) + 5
      ) % 7
    ) IN (5, 6)
  ) AS weekend_payments
FROM main.payments_received AS payments_received
JOIN main.sales AS sales
  ON payments_received.sale_id = sales._id AND sales.sale_price > 30000
WHERE
  CAST(DATEDIFF(
    DAY,
    DATEADD(
      DAY,
      -(
        (
          DAYOFWEEK(TO_DATE(payments_received.payment_date)) + 5
        ) % 7
      ),
      CAST(payments_received.payment_date AS DATE)
    ),
    DATEADD(
      DAY,
      -(
        (
          DAYOFWEEK(TO_DATE(CURRENT_TIMESTAMP())) + 5
        ) % 7
      ),
      CAST(CURRENT_TIMESTAMP() AS DATE)
    )
  ) / 7 AS BIGINT) <= 8
  AND CAST(DATEDIFF(
    DAY,
    DATEADD(
      DAY,
      -(
        (
          DAYOFWEEK(TO_DATE(payments_received.payment_date)) + 5
        ) % 7
      ),
      CAST(payments_received.payment_date AS DATE)
    ),
    DATEADD(
      DAY,
      -(
        (
          DAYOFWEEK(TO_DATE(CURRENT_TIMESTAMP())) + 5
        ) % 7
      ),
      CAST(CURRENT_TIMESTAMP() AS DATE)
    )
  ) / 7 AS BIGINT) >= 1
GROUP BY
  1
