SELECT
  payment_week,
  COALESCE(agg_0, 0) AS total_payments,
  COALESCE(agg_1, 0) AS weekend_payments
FROM (
  SELECT
    _table_alias_0.payment_week AS payment_week,
    agg_0,
    agg_1
  FROM (
    SELECT
      COUNT() AS agg_0,
      payment_week
    FROM (
      SELECT
        DATE(
          payment_date,
          '-' || CAST((
            CAST(STRFTIME('%w', DATETIME(payment_date)) AS INTEGER) + 6
          ) % 7 AS TEXT) || ' days',
          'start of day'
        ) AS payment_week
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
              CAST(CAST(CAST((JULIANDAY(DATE(
                DATE(
                  DATE(
                    'now',
                    '-' || CAST((
                      CAST(STRFTIME('%w', DATETIME('now')) AS INTEGER) + 6
                    ) % 7 AS TEXT) || ' days',
                    'start of day'
                  ),
                  '-' || CAST((
                    CAST(STRFTIME(
                      '%w',
                      DATETIME(
                        DATE(
                          'now',
                          '-' || CAST((
                            CAST(STRFTIME('%w', DATETIME('now')) AS INTEGER) + 6
                          ) % 7 AS TEXT) || ' days',
                          'start of day'
                        )
                      )
                    ) AS INTEGER) + 6
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
              ))) AS INTEGER) AS REAL) / 7 AS INTEGER) >= 1
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
    GROUP BY
      payment_week
  ) AS _table_alias_0
  LEFT JOIN (
    SELECT
      COUNT() AS agg_1,
      payment_week
    FROM (
      SELECT
        DATE(
          payment_date,
          '-' || CAST((
            CAST(STRFTIME('%w', DATETIME(payment_date)) AS INTEGER) + 6
          ) % 7 AS TEXT) || ' days',
          'start of day'
        ) AS payment_week
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
              CAST(CAST(CAST((JULIANDAY(DATE(
                DATE(
                  DATE(
                    'now',
                    '-' || CAST((
                      CAST(STRFTIME('%w', DATETIME('now')) AS INTEGER) + 6
                    ) % 7 AS TEXT) || ' days',
                    'start of day'
                  ),
                  '-' || CAST((
                    CAST(STRFTIME(
                      '%w',
                      DATETIME(
                        DATE(
                          'now',
                          '-' || CAST((
                            CAST(STRFTIME('%w', DATETIME('now')) AS INTEGER) + 6
                          ) % 7 AS TEXT) || ' days',
                          'start of day'
                        )
                      )
                    ) AS INTEGER) + 6
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
              ))) AS INTEGER) AS REAL) / 7 AS INTEGER) >= 1
            )
            AND (
              (
                (
                  (
                    (
                      CAST(STRFTIME('%w', payment_date) AS INTEGER) + 6
                    ) % 7
                  )
                ) = 6
              )
              OR (
                (
                  (
                    (
                      CAST(STRFTIME('%w', payment_date) AS INTEGER) + 6
                    ) % 7
                  )
                ) = 7
              )
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
    GROUP BY
      payment_week
  ) AS _table_alias_1
    ON _table_alias_0.payment_week = _table_alias_1.payment_week
)
