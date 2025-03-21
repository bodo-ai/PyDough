SELECT
  _id,
  symbol
FROM (
  SELECT
    sbTickerId AS _id,
    sbTickerSymbol AS symbol
  FROM main.sbTicker
)
WHERE
  NOT EXISTS(
    SELECT
      1
    FROM (
      SELECT
        sbDpTickerId AS ticker_id
      FROM main.sbDailyPrice
    )
    WHERE
      _id = ticker_id
  )
