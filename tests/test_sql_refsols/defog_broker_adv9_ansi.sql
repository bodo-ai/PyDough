WITH _table_alias_0 AS (
  SELECT
    sbtransaction.sbtxdatetime AS date_time,
    sbtransaction.sbtxtickerid AS ticker_id
  FROM main.sbtransaction AS sbtransaction
  WHERE
    sbtransaction.sbtxdatetime < DATE_TRUNC('WEEK', CURRENT_TIMESTAMP())
    AND sbtransaction.sbtxdatetime >= DATE_ADD(DATE_TRUNC('WEEK', CURRENT_TIMESTAMP()), -8, 'WEEK')
), _table_alias_1 AS (
  SELECT
    sbticker.sbtickerid AS _id
  FROM main.sbticker AS sbticker
  WHERE
    sbticker.sbtickertype = 'stock'
), _t0 AS (
  SELECT
    COUNT() AS agg_0,
    SUM(CAST(DAY_OF_WEEK(_table_alias_0.date_time) AS INT) IN (5, 6)) AS agg_1,
    DATE_TRUNC('WEEK', CAST(_table_alias_0.date_time AS TIMESTAMP)) AS week
  FROM _table_alias_0 AS _table_alias_0
  JOIN _table_alias_1 AS _table_alias_1
    ON _table_alias_0.ticker_id = _table_alias_1._id
  GROUP BY
    DATE_TRUNC('WEEK', CAST(_table_alias_0.date_time AS TIMESTAMP))
)
SELECT
  _t0.week AS week,
  COALESCE(_t0.agg_0, 0) AS num_transactions,
  COALESCE(_t0.agg_1, 0) AS weekend_transactions
FROM _t0 AS _t0
