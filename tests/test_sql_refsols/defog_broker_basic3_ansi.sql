SELECT
  symbol,
  num_transactions,
  total_amount
FROM (
  SELECT
    COALESCE(agg_0, 0) AS num_transactions,
    COALESCE(agg_1, 0) AS total_amount,
    symbol
  FROM (
    SELECT
      agg_0,
      agg_1,
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
        SUM(amount) AS agg_1,
        ticker_id
      FROM (
        SELECT
          sbTxAmount AS amount,
          sbTxTickerId AS ticker_id
        FROM main.sbTransaction
      )
      GROUP BY
        ticker_id
    )
      ON _id = ticker_id
  )
)
ORDER BY
  total_amount DESC
LIMIT 10
