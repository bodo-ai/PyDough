SELECT
  symbol,
  month,
  avg_close,
  max_high,
  min_low,
  CAST((
    avg_close - LAG(avg_close, 1) OVER (PARTITION BY symbol ORDER BY month)
  ) AS REAL) / LAG(avg_close, 1) OVER (PARTITION BY symbol ORDER BY month) AS momc
FROM (
  SELECT
    _table_alias_4.month AS month,
    _table_alias_4.symbol AS symbol,
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
          CAST(STRFTIME('%Y', date) AS INTEGER),
          CASE
            WHEN LENGTH(CAST(STRFTIME('%m', date) AS INTEGER)) >= 2
            THEN SUBSTRING(CAST(STRFTIME('%m', date) AS INTEGER), 1, 2)
            ELSE SUBSTRING('00' || CAST(STRFTIME('%m', date) AS INTEGER), (
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
        ) AS _table_alias_0
        LEFT JOIN (
          SELECT
            sbTickerId AS _id,
            sbTickerSymbol AS symbol
          FROM main.sbTicker
        ) AS _table_alias_1
          ON ticker_id = _id
      ) AS _t2
    ) AS _t1
  ) AS _table_alias_4
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
          CAST(STRFTIME('%Y', date) AS INTEGER),
          CASE
            WHEN LENGTH(CAST(STRFTIME('%m', date) AS INTEGER)) >= 2
            THEN SUBSTRING(CAST(STRFTIME('%m', date) AS INTEGER), 1, 2)
            ELSE SUBSTRING('00' || CAST(STRFTIME('%m', date) AS INTEGER), (
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
        ) AS _table_alias_2
        LEFT JOIN (
          SELECT
            sbTickerId AS _id,
            sbTickerSymbol AS symbol
          FROM main.sbTicker
        ) AS _table_alias_3
          ON ticker_id = _id
      ) AS _t4
    ) AS _t3
    GROUP BY
      month,
      symbol
  ) AS _table_alias_5
    ON (
      _table_alias_4.symbol = _table_alias_5.symbol
    )
    AND (
      _table_alias_4.month = _table_alias_5.month
    )
) AS _t0
