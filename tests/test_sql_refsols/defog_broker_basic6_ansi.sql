SELECT
  _id
FROM (
  SELECT
    sbTickerId AS _id
  FROM main.sbTicker
) AS _table_alias_0
SEMI JOIN (
  SELECT
    ticker_id
  FROM (
    SELECT
      sbDpDate AS date,
      sbDpTickerId AS ticker_id
    FROM main.sbDailyPrice
  ) AS _t0
  WHERE
    date >= CAST('2023-04-01' AS DATE)
) AS _table_alias_1
  ON _id = ticker_id
