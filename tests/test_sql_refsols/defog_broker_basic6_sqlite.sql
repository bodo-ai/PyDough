SELECT
  _id
FROM (
  SELECT
    sbTickerId AS _id
  FROM main.sbTicker
)
WHERE
  EXISTS(
    SELECT
      1
    FROM (
      SELECT
        ticker_id
      FROM (
        SELECT
          sbDpDate AS date,
          sbDpTickerId AS ticker_id
        FROM main.sbDailyPrice
      )
      WHERE
        date >= '2023-04-01'
    )
    WHERE
      _id = ticker_id
  )
