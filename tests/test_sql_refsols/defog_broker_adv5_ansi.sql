SELECT
  symbol,
  month,
  avg_close,
  max_high,
  min_low,
  (
    avg_close - LAG(avg_close, 1) OVER (PARTITION BY symbol ORDER BY month NULLS LAST)
  ) / LAG(avg_close, 1) OVER (PARTITION BY symbol ORDER BY month NULLS LAST) AS momc
FROM (
  SELECT
    _table_alias_0.month AS month,
    _table_alias_0.symbol AS symbol,
    agg_0 AS avg_close,
    agg_1 AS max_high,
    agg_2 AS min_low
  FROM (
    SELECT DISTINCT
      month,
      symbol
    FROM (
      SELECT
        CONCAT_WS(
          '-',
          YEAR(date),
          CASE
            WHEN LENGTH(MONTH(date)) >= 2
            THEN SUBSTRING(MONTH(date), 1, 2)
            ELSE SUBSTRING(CONCAT('00', MONTH(date)), (
              2 * -1
            ))
          END
        ) AS month,
        symbol
      FROM (
        SELECT
          date,
          symbol
        FROM (
          SELECT
            sbDpDate AS date,
            sbDpTickerId AS ticker_id
          FROM main.sbDailyPrice
        )
        LEFT JOIN (
          SELECT
            sbTickerId AS _id,
            sbTickerSymbol AS symbol
          FROM main.sbTicker
        )
          ON ticker_id = _id
      )
    )
  ) AS _table_alias_0
  LEFT JOIN (
    SELECT
      AVG(close) AS agg_0,
      MAX(high) AS agg_1,
      MIN(low) AS agg_2,
      month,
      symbol
    FROM (
      SELECT
        CONCAT_WS(
          '-',
          YEAR(date),
          CASE
            WHEN LENGTH(MONTH(date)) >= 2
            THEN SUBSTRING(MONTH(date), 1, 2)
            ELSE SUBSTRING(CONCAT('00', MONTH(date)), (
              2 * -1
            ))
          END
        ) AS month,
        close,
        high,
        low,
        symbol
      FROM (
        SELECT
          close,
          date,
          high,
          low,
          symbol
        FROM (
          SELECT
            sbDpClose AS close,
            sbDpDate AS date,
            sbDpHigh AS high,
            sbDpLow AS low,
            sbDpTickerId AS ticker_id
          FROM main.sbDailyPrice
        )
        LEFT JOIN (
          SELECT
            sbTickerId AS _id,
            sbTickerSymbol AS symbol
          FROM main.sbTicker
        )
          ON ticker_id = _id
      )
    )
    GROUP BY
      month,
      symbol
  ) AS _table_alias_1
    ON (
      _table_alias_0.symbol = _table_alias_1.symbol
    )
    AND (
      _table_alias_0.month = _table_alias_1.month
    )
)
