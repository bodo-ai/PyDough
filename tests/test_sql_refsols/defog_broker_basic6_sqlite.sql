SELECT
  _id
FROM (
  SELECT
    sbTickerId AS _id
  FROM main.sbTicker
) AS _table_alias_0
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
      ) AS _t0
      WHERE
        date >= '2023-04-01'
    ) AS _table_alias_1
    WHERE
      _id = ticker_id
  )
