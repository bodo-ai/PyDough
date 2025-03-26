WITH _table_alias_0 AS (
  SELECT
    sbticker.sbtickerid AS _id,
    sbticker.sbtickersymbol AS symbol
  FROM main.sbticker AS sbticker
), _table_alias_1 AS (
  SELECT
    SUM(sbtransaction.sbtxtax + sbtransaction.sbtxcommission) AS agg_1,
    SUM(sbtransaction.sbtxamount) AS agg_0,
    sbtransaction.sbtxtickerid AS ticker_id
  FROM main.sbtransaction AS sbtransaction
  WHERE
    sbtransaction.sbtxdatetime >= DATE_ADD(CURRENT_TIMESTAMP(), -1, 'MONTH')
    AND sbtransaction.sbtxtype = 'sell'
  GROUP BY
    sbtransaction.sbtxtickerid
)
SELECT
  _table_alias_0.symbol AS symbol,
  (
    100.0 * (
      COALESCE(_table_alias_1.agg_0, 0) - COALESCE(_table_alias_1.agg_1, 0)
    )
  ) / COALESCE(_table_alias_1.agg_0, 0) AS SPM
FROM _table_alias_0 AS _table_alias_0
LEFT JOIN _table_alias_1 AS _table_alias_1
  ON _table_alias_0._id = _table_alias_1.ticker_id
WHERE
  NOT (
    100.0 * (
      COALESCE(_table_alias_1.agg_0, 0) - COALESCE(_table_alias_1.agg_1, 0)
    )
  ) / COALESCE(_table_alias_1.agg_0, 0) IS NULL
ORDER BY
  _table_alias_0.symbol
