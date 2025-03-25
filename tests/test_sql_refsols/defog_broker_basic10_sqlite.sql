SELECT
  _id,
  symbol
FROM (
  SELECT
    sbTickerId AS _id,
    sbTickerSymbol AS symbol
  FROM main.sbTicker
) AS _table_alias_0
WHERE
  NOT EXISTS(
    SELECT
      1
    FROM (
      SELECT
        sbDpTickerId AS ticker_id
      FROM main.sbDailyPrice
    ) AS _table_alias_1
    WHERE
      _id = ticker_id
  )
