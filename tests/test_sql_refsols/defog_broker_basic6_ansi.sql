SELECT
  _id
FROM (
  SELECT
    sbTickerId AS _id
  FROM main.sbTicker
)
SEMI JOIN (
  SELECT
    ticker_id
  FROM (
    SELECT
      sbDpDate AS date,
      sbDpTickerId AS ticker_id
    FROM main.sbDailyPrice
  )
  WHERE
    date >= CAST('2023-04-01' AS DATE)
)
  ON _id = ticker_id
