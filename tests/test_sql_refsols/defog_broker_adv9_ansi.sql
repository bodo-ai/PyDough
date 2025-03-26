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
      DATE_TRUNC('WEEK', CAST(date_time AS TIMESTAMP)) AS week,
      DAY_OF_WEEK(date_time) IN (5, 6) AS is_weekend
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
            date_time < DATE_TRUNC('WEEK', CURRENT_TIMESTAMP())
          )
          AND (
            date_time >= DATE_ADD(DATE_TRUNC('WEEK', CURRENT_TIMESTAMP()), -8, 'WEEK')
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
