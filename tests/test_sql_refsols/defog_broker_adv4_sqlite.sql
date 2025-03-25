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
      ) AS _table_alias_0
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
          ) AS _t4
          WHERE
            (
              date <= '2023-04-04'
            ) AND (
              date >= '2023-04-01'
            )
        ) AS _t3
        GROUP BY
          ticker_id
      ) AS _table_alias_1
        ON _id = ticker_id
    ) AS _t2
  ) AS _t1
  ORDER BY
    ordering_2 DESC
  LIMIT 3
) AS _t0
ORDER BY
  ordering_2 DESC
