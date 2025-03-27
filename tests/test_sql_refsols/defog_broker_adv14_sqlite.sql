SELECT
  ticker_type,
  AVG(close) AS ACP
FROM (
  SELECT
    close,
    ticker_type
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
      CAST((JULIANDAY(DATE(DATETIME('now'), 'start of day')) - JULIANDAY(DATE(date, 'start of day'))) AS INTEGER) <= 7
  )
  LEFT JOIN (
    SELECT
      sbTickerId AS _id,
      sbTickerType AS ticker_type
    FROM main.sbTicker
  )
    ON ticker_id = _id
)
GROUP BY
  ticker_type
