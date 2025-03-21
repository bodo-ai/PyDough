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
    )
    WHERE
      DATEDIFF('now', date, DAY) <= 7
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
