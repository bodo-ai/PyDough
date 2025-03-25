SELECT
  MIN(close) AS lowest_price
FROM (
  SELECT
    close
  FROM (
    SELECT
      close,
      ticker_id
    FROM (
      SELECT
        sbDpClose AS close,
        sbDpDate AS date,
        sbDpTickerId AS ticker_id
      FROM main.sbDailyPrice
    ) AS _t1
    WHERE
      CAST((JULIANDAY(DATE(DATETIME('now'), 'start of day')) - JULIANDAY(DATE(date, 'start of day'))) AS INTEGER) <= 7
  ) AS _table_alias_0
  INNER JOIN (
    SELECT
      _id
    FROM (
      SELECT
        sbTickerId AS _id,
        sbTickerSymbol AS symbol
      FROM main.sbTicker
    ) AS _t2
    WHERE
      symbol = 'VTI'
  ) AS _table_alias_1
    ON ticker_id = _id
) AS _t0
