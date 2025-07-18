SELECT
  DATE(
    payments_received.payment_date,
    '-' || CAST((
      CAST(STRFTIME('%w', DATETIME(payments_received.payment_date)) AS INTEGER) + 6
    ) % 7 AS TEXT) || ' days',
    'start of day'
  ) AS payment_week,
  COUNT(*) AS total_payments,
  COALESCE(
    SUM(
      (
        (
          CAST(STRFTIME('%w', payments_received.payment_date) AS INTEGER) + 6
        ) % 7
      ) IN (5, 6)
    ),
    0
  ) AS weekend_payments
FROM main.payments_received AS payments_received
JOIN main.sales AS sales
  ON payments_received.sale_id = sales._id AND sales.sale_price > 30000
WHERE
  CAST(CAST(CAST((
    JULIANDAY(
      DATE(
        DATETIME('now'),
        '-' || CAST((
          CAST(STRFTIME('%w', DATETIME(DATETIME('now'))) AS INTEGER) + 6
        ) % 7 AS TEXT) || ' days',
        'start of day'
      )
    ) - JULIANDAY(
      DATE(
        payments_received.payment_date,
        '-' || CAST((
          CAST(STRFTIME('%w', DATETIME(payments_received.payment_date)) AS INTEGER) + 6
        ) % 7 AS TEXT) || ' days',
        'start of day'
      )
    )
  ) AS INTEGER) AS REAL) / 7 AS INTEGER) <= 8
  AND CAST(CAST(CAST((
    JULIANDAY(
      DATE(
        DATETIME('now'),
        '-' || CAST((
          CAST(STRFTIME('%w', DATETIME(DATETIME('now'))) AS INTEGER) + 6
        ) % 7 AS TEXT) || ' days',
        'start of day'
      )
    ) - JULIANDAY(
      DATE(
        payments_received.payment_date,
        '-' || CAST((
          CAST(STRFTIME('%w', DATETIME(payments_received.payment_date)) AS INTEGER) + 6
        ) % 7 AS TEXT) || ' days',
        'start of day'
      )
    )
  ) AS INTEGER) AS REAL) / 7 AS INTEGER) >= 1
GROUP BY
  DATE(
    payments_received.payment_date,
    '-' || CAST((
      CAST(STRFTIME('%w', DATETIME(payments_received.payment_date)) AS INTEGER) + 6
    ) % 7 AS TEXT) || ' days',
    'start of day'
  )
