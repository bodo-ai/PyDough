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
    ) AS _t1
    WHERE
      DATEDIFF(CURRENT_TIMESTAMP(), date, DAY) <= 7
  ) AS _table_alias_0
  LEFT JOIN (
    SELECT
      sbTickerId AS _id,
      sbTickerType AS ticker_type
    FROM main.sbTicker
  ) AS _table_alias_1
    ON ticker_id = _id
) AS _t0
GROUP BY
  ticker_type
