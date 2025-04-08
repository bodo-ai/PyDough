SELECT
  payment_week,
  COALESCE(agg_0, 0) AS total_payments,
  COALESCE(agg_1, 0) AS weekend_payments
FROM (
  SELECT
    COUNT() AS agg_0,
    SUM(expr_2) AS agg_1,
    payment_week
  FROM (
    SELECT
      (
        (
          (
            CAST(STRFTIME('%w', payment_date) AS INTEGER) + 6
          ) % 7
        )
      ) IN (0, 6) AS expr_2,
      payment_week
    FROM (
      SELECT
        DATE(
          payment_date,
          '-' || CAST((
            CAST(STRFTIME('%w', DATETIME(payment_date)) AS INTEGER) + 6
          ) % 7 AS TEXT) || ' days',
          'start of day'
        ) AS payment_week,
        payment_date
      FROM (
        SELECT
          payment_date
        FROM (
          SELECT
            payment_date,
            sale_id
          FROM main.payments_received
          WHERE
            (
              1 <= CAST(CAST(CAST((JULIANDAY(DATE(
                DATE(
                  DATETIME('now'),
                  '-' || CAST((
                    CAST(STRFTIME('%w', DATETIME(DATETIME('now'))) AS INTEGER) + 6
                  ) % 7 AS TEXT) || ' days',
                  'start of day'
                ),
                'start of day'
              )) - JULIANDAY(DATE(
                DATE(
                  payment_date,
                  '-' || CAST((
                    CAST(STRFTIME('%w', DATETIME(payment_date)) AS INTEGER) + 6
                  ) % 7 AS TEXT) || ' days',
                  'start of day'
                ),
                'start of day'
              ))) AS INTEGER) AS REAL) / 7 AS INTEGER)
            )
            AND (
              CAST(CAST(CAST((JULIANDAY(DATE(
                DATE(
                  DATETIME('now'),
                  '-' || CAST((
                    CAST(STRFTIME('%w', DATETIME(DATETIME('now'))) AS INTEGER) + 6
                  ) % 7 AS TEXT) || ' days',
                  'start of day'
                ),
                'start of day'
              )) - JULIANDAY(DATE(
                DATE(
                  payment_date,
                  '-' || CAST((
                    CAST(STRFTIME('%w', DATETIME(payment_date)) AS INTEGER) + 6
                  ) % 7 AS TEXT) || ' days',
                  'start of day'
                ),
                'start of day'
              ))) AS INTEGER) AS REAL) / 7 AS INTEGER) <= 8
            )
        )
        INNER JOIN (
          SELECT
            _id
          FROM (
            SELECT
              _id,
              sale_price
            FROM main.sales
          )
          WHERE
            sale_price > 30000
        )
          ON sale_id = _id
      )
    )
  )
  GROUP BY
    payment_week
)
