SELECT
  symbol,
  tx_count
FROM (
  SELECT
    COALESCE(agg_0, 0) AS tx_count,
    symbol
  FROM (
    SELECT
      agg_0,
      symbol
    FROM (
      SELECT
        sbTickerId AS _id,
        sbTickerSymbol AS symbol
      FROM main.sbTicker
    )
    LEFT JOIN (
      SELECT
        COUNT() AS agg_0,
        ticker_id
      FROM (
        SELECT
          ticker_id
        FROM (
          SELECT
            sbTxDateTime AS date_time,
            sbTxTickerId AS ticker_id,
            sbTxType AS transaction_type
          FROM main.sbTransaction
        )
        WHERE
          (
            transaction_type = 'buy'
          )
          AND (
            date_time >= DATE(DATETIME('now', '-10 day'), 'start of day')
          )
      )
      GROUP BY
        ticker_id
    )
      ON _id = ticker_id
  )
)
ORDER BY
  tx_count DESC
LIMIT 2
