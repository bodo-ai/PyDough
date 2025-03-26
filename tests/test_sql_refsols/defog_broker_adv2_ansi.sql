WITH _table_alias_0 AS (
  SELECT
    sbticker.sbtickerid AS _id,
    sbticker.sbtickersymbol AS symbol
  FROM main.sbticker AS sbticker
), _table_alias_1 AS (
  SELECT
    COUNT() AS agg_0,
    sbtransaction.sbtxtickerid AS ticker_id
  FROM main.sbtransaction AS sbtransaction
  WHERE
    sbtransaction.sbtxdatetime >= DATE_TRUNC('DAY', DATE_ADD(CURRENT_TIMESTAMP(), -10, 'DAY'))
    AND sbtransaction.sbtxtype = 'buy'
  GROUP BY
    sbtransaction.sbtxtickerid
), _t0 AS (
  SELECT
    COALESCE(_table_alias_1.agg_0, 0) AS ordering_1,
    _table_alias_0.symbol AS symbol,
    COALESCE(_table_alias_1.agg_0, 0) AS tx_count
  FROM _table_alias_0 AS _table_alias_0
  LEFT JOIN _table_alias_1 AS _table_alias_1
    ON _table_alias_0._id = _table_alias_1.ticker_id
  ORDER BY
    ordering_1 DESC
  LIMIT 2
)
SELECT
  _t0.symbol AS symbol,
  _t0.tx_count AS tx_count
FROM _t0 AS _t0
ORDER BY
  _t0.ordering_1 DESC
