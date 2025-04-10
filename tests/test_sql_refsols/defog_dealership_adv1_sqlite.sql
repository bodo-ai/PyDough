WITH _t0_2 AS (
  SELECT
    COUNT() AS agg_0,
    SUM(
      (
        (
          CAST(STRFTIME('%w', payments_received.payment_date) AS INTEGER) + 6
        ) % 7
      ) IN (5, 6)
    ) AS agg_1,
    DATE(
      payments_received.payment_date,
      '-' || CAST((
        CAST(STRFTIME('%w', DATETIME(payments_received.payment_date)) AS INTEGER) + 6
      ) % 7 AS TEXT) || ' days',
      'start of day'
    ) AS payment_week
  FROM main.payments_received AS payments_received
  JOIN main.sales AS sales
    ON payments_received.sale_id = sales._id AND sales.sale_price > 30000
  WHERE
    CAST(CAST(CAST((
      JULIANDAY(
        DATE(
          DATE(
            DATETIME('now'),
            '-' || CAST((
              CAST(STRFTIME('%w', DATETIME(DATETIME('now'))) AS INTEGER) + 6
            ) % 7 AS TEXT) || ' days',
            'start of day'
          ),
          'start of day'
        )
      ) - JULIANDAY(
        DATE(
          DATE(
            payments_received.payment_date,
            '-' || CAST((
              CAST(STRFTIME('%w', DATETIME(payments_received.payment_date)) AS INTEGER) + 6
            ) % 7 AS TEXT) || ' days',
            'start of day'
          ),
          'start of day'
        )
      )
    ) AS INTEGER) AS REAL) / 7 AS INTEGER) <= 8
    AND CAST(CAST(CAST((
      JULIANDAY(
        DATE(
          DATE(
            DATETIME('now'),
            '-' || CAST((
              CAST(STRFTIME('%w', DATETIME(DATETIME('now'))) AS INTEGER) + 6
            ) % 7 AS TEXT) || ' days',
            'start of day'
          ),
          'start of day'
        )
      ) - JULIANDAY(
        DATE(
          DATE(
            payments_received.payment_date,
            '-' || CAST((
              CAST(STRFTIME('%w', DATETIME(payments_received.payment_date)) AS INTEGER) + 6
            ) % 7 AS TEXT) || ' days',
            'start of day'
          ),
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
)
SELECT
  payment_week,
  COALESCE(agg_0, 0) AS total_payments,
  COALESCE(agg_1, 0) AS weekend_payments
FROM _t0_2
