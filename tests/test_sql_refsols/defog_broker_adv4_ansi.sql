SELECT
  symbol,
  price_change
FROM (
  SELECT
    ordering_2,
    price_change,
    symbol
  FROM (
    SELECT
      high - low AS ordering_2,
      high - low AS price_change,
      symbol
    FROM (
      SELECT
        agg_0 AS high,
        agg_1 AS low,
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
              date <= DATE_STR_TO_DATE('2023-04-04')
            )
            AND (
              date >= DATE_STR_TO_DATE('2023-04-01')
            )
        )
        GROUP BY
          ticker_id
      )
        ON _id = ticker_id
    )
  )
  ORDER BY
    ordering_2 DESC
  LIMIT 3
)
ORDER BY
  ordering_2 DESC
