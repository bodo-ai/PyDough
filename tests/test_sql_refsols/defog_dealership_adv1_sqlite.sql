WITH _s0 AS (
  SELECT
    COUNT(*) AS agg_0,
    SUM((
      (
        CAST(STRFTIME('%w', payment_date) AS INTEGER) + 6
      ) % 7
    ) IN (5, 6)) AS agg_1,
    DATE(
      payment_date,
      '-' || CAST((
        CAST(STRFTIME('%w', DATETIME(payment_date)) AS INTEGER) + 6
      ) % 7 AS TEXT) || ' days',
      'start of day'
    ) AS payment_week,
    sale_id
  FROM main.payments_received
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
            payment_date,
            '-' || CAST((
              CAST(STRFTIME('%w', DATETIME(payment_date)) AS INTEGER) + 6
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
            payment_date,
            '-' || CAST((
              CAST(STRFTIME('%w', DATETIME(payment_date)) AS INTEGER) + 6
            ) % 7 AS TEXT) || ' days',
            'start of day'
          ),
          'start of day'
        )
      )
    ) AS INTEGER) AS REAL) / 7 AS INTEGER) >= 1
  GROUP BY
    DATE(
      payment_date,
      '-' || CAST((
        CAST(STRFTIME('%w', DATETIME(payment_date)) AS INTEGER) + 6
      ) % 7 AS TEXT) || ' days',
      'start of day'
    ),
    sale_id
), _t0 AS (
  SELECT
    SUM(_s0.agg_0) AS agg_0,
    SUM(_s0.agg_1) AS agg_1,
    _s0.payment_week
  FROM _s0 AS _s0
  JOIN main.sales AS sales
    ON _s0.sale_id = sales._id AND sales.sale_price > 30000
  GROUP BY
    _s0.payment_week
)
SELECT
  payment_week,
  agg_0 AS total_payments,
  COALESCE(agg_1, 0) AS weekend_payments
FROM _t0
