SELECT
  _id,
  symbol
FROM (
  SELECT
    sbTickerId AS _id,
    sbTickerSymbol AS symbol
  FROM main.sbTicker
)
ANTI JOIN (
  SELECT
    sbDpTickerId AS ticker_id
  FROM main.sbDailyPrice
)
  ON _id = ticker_id
