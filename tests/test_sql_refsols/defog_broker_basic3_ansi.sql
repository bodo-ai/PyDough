WITH _table_alias_1 AS (
  SELECT
    COUNT() AS agg_0,
    SUM(sbtransaction.sbtxamount) AS agg_1,
    sbtransaction.sbtxtickerid AS ticker_id
  FROM main.sbtransaction AS sbtransaction
  GROUP BY
    sbtransaction.sbtxtickerid
), _t0 AS (
  SELECT
    COALESCE(_table_alias_1.agg_0, 0) AS num_transactions,
    COALESCE(_table_alias_1.agg_1, 0) AS ordering_2,
    sbticker.sbtickersymbol AS symbol,
    COALESCE(_table_alias_1.agg_1, 0) AS total_amount
  FROM main.sbticker AS sbticker
  LEFT JOIN _table_alias_1 AS _table_alias_1
    ON _table_alias_1.ticker_id = sbticker.sbtickerid
  ORDER BY
    ordering_2 DESC
  LIMIT 10
)
SELECT
  _t0.symbol AS symbol,
  _t0.num_transactions AS num_transactions,
  _t0.total_amount AS total_amount
FROM _t0 AS _t0
ORDER BY
  _t0.ordering_2 DESC
