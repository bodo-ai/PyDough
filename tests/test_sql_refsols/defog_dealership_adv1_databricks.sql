SELECT
  DATE_ADD(
    CAST(CAST(payments_received.payment_date AS TIMESTAMP) AS DATE),
    -(
      (
        DAYOFWEEK(CAST(payments_received.payment_date AS TIMESTAMP)) + 5
      ) % 7
    )
  ) AS payment_week,
  COUNT(*) AS total_payments,
  COALESCE(
    COUNT_IF((
      (
        DAYOFWEEK(payments_received.payment_date) + 5
      ) % 7
    ) IN (5, 6)),
    0
  ) AS weekend_payments
FROM defog.dealership.payments_received AS payments_received
JOIN defog.dealership.sales AS sales
  ON payments_received.sale_id = sales.id AND sales.sale_price > 30000
WHERE
  CAST(DATEDIFF(
    DAY,
    DATE_ADD(
      CAST(payments_received.payment_date AS DATE),
      -(
        (
          DAYOFWEEK(payments_received.payment_date) + 5
        ) % 7
      )
    ),
    DATE_ADD(
      CAST(CURRENT_TIMESTAMP() AS DATE),
      -(
        (
          DAYOFWEEK(CURRENT_TIMESTAMP()) + 5
        ) % 7
      )
    )
  ) / 7 AS BIGINT) <= 8
  AND CAST(DATEDIFF(
    DAY,
    DATE_ADD(
      CAST(payments_received.payment_date AS DATE),
      -(
        (
          DAYOFWEEK(payments_received.payment_date) + 5
        ) % 7
      )
    ),
    DATE_ADD(
      CAST(CURRENT_TIMESTAMP() AS DATE),
      -(
        (
          DAYOFWEEK(CURRENT_TIMESTAMP()) + 5
        ) % 7
      )
    )
  ) / 7 AS BIGINT) >= 1
GROUP BY
  1
