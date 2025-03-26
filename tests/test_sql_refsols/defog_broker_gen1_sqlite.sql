SELECT
  MIN(close) AS lowest_price
FROM (
  SELECT
    close
  FROM (
    SELECT
      sbDpClose AS close,
      sbDpTickerId AS ticker_id
    FROM main.sbDailyPrice
    WHERE
      CAST((JULIANDAY(DATE(DATETIME('now'), 'start of day')) - JULIANDAY(DATE(date, 'start of day'))) AS INTEGER) <= 7
  )
  INNER JOIN (
    SELECT
      _id
    FROM (
      SELECT
        sbTickerId AS _id,
        sbTickerSymbol AS symbol
      FROM main.sbTicker
    )
    WHERE
      symbol = 'VTI'
  )
    ON ticker_id = _id
)
