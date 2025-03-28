SELECT
  symbol,
  price_change
FROM (
  SELECT
    agg_0 - agg_1 AS price_change,
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
        MAX(high) AS agg_0,
        MIN(low) AS agg_1,
        ticker_id
      FROM (
        SELECT
          high,
          low,
          ticker_id
        FROM (
          SELECT
            sbDpDate AS date,
            sbDpHigh AS high,
            sbDpLow AS low,
            sbDpTickerId AS ticker_id
          FROM main.sbDailyPrice
        )
        WHERE
          (
            date <= CAST('2023-04-04' AS DATE)
          )
          AND (
            date >= CAST('2023-04-01' AS DATE)
          )
      )
      GROUP BY
        ticker_id
    )
      ON _id = ticker_id
  )
)
ORDER BY
  price_change DESC
LIMIT 3
