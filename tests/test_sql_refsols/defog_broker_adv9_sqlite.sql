SELECT
  week,
  COALESCE(agg_0, 0) AS num_transactions,
  COALESCE(agg_1, 0) AS weekend_transactions
FROM (
  SELECT
    COUNT() AS agg_0,
    SUM(is_weekend) AS agg_1,
    week
  FROM (
    SELECT
      DATE(
        date_time,
        '-' || (
          (
            CAST(STRFTIME('%w', DATETIME(date_time)) AS INTEGER) + 6
          ) % 7
        ) || ' days',
        'start of day'
      ) AS week,
      (
        (
          (
            CAST(STRFTIME('%w', date_time) AS INTEGER) + 6
          ) % 7
        )
      ) IN (5, 6) AS is_weekend
    FROM (
      SELECT
        date_time
      FROM (
        SELECT
          date_time,
          ticker_id
        FROM (
          SELECT
            sbTxDateTime AS date_time,
            sbTxTickerId AS ticker_id
          FROM main.sbTransaction
        )
        WHERE
          (
            date_time < DATE(
              'now',
              '-' || (
                (
                  CAST(STRFTIME('%w', DATETIME('now')) AS INTEGER) + 6
                ) % 7
              ) || ' days',
              'start of day'
            )
          )
          AND (
            date_time >= DATE(
              'now',
              '-' || (
                (
                  CAST(STRFTIME('%w', DATETIME('now')) AS INTEGER) + 6
                ) % 7
              ) || ' days',
              'start of day',
              '-56 day'
            )
          )
      )
      INNER JOIN (
        SELECT
          _id
        FROM (
          SELECT
            sbTickerId AS _id,
            sbTickerType AS ticker_type
          FROM main.sbTicker
        )
        WHERE
          ticker_type = 'stock'
      )
        ON ticker_id = _id
    )
  )
  GROUP BY
    week
)
