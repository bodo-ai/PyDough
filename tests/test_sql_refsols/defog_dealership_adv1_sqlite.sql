WITH _t0 AS (
  SELECT
    COUNT(*) AS agg_0,
    SUM(
      (
        (
          CAST(STRFTIME('%w', _s0.payment_date) AS INTEGER) + 6
        ) % 7
      ) IN (5, 6)
    ) AS agg_1,
    DATE(
      _s0.payment_date,
      '-' || CAST((
        CAST(STRFTIME('%w', DATETIME(_s0.payment_date)) AS INTEGER) + 6
      ) % 7 AS TEXT) || ' days',
      'start of day'
    ) AS payment_week
  FROM main.payments_received AS _s0
  JOIN main.sales AS _s1
    ON _s0.sale_id = _s1._id AND _s1.sale_price > 30000
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
            _s0.payment_date,
            '-' || CAST((
              CAST(STRFTIME('%w', DATETIME(_s0.payment_date)) AS INTEGER) + 6
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
            _s0.payment_date,
            '-' || CAST((
              CAST(STRFTIME('%w', DATETIME(_s0.payment_date)) AS INTEGER) + 6
            ) % 7 AS TEXT) || ' days',
            'start of day'
          ),
          'start of day'
        )
      )
    ) AS INTEGER) AS REAL) / 7 AS INTEGER) >= 1
  GROUP BY
    DATE(
      _s0.payment_date,
      '-' || CAST((
        CAST(STRFTIME('%w', DATETIME(_s0.payment_date)) AS INTEGER) + 6
      ) % 7 AS TEXT) || ' days',
      'start of day'
    )
)
SELECT
  payment_week,
  agg_0 AS total_payments,
  COALESCE(agg_1, 0) AS weekend_payments
FROM _t0
