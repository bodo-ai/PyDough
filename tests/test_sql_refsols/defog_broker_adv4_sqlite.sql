WITH _table_alias_0 AS (
  SELECT
    sbticker.sbtickerid AS _id,
    sbticker.sbtickersymbol AS symbol
  FROM main.sbticker AS sbticker
), _table_alias_1 AS (
  SELECT
    MAX(sbdailyprice.sbdphigh) AS agg_0,
    MIN(sbdailyprice.sbdplow) AS agg_1,
    sbdailyprice.sbdptickerid AS ticker_id
  FROM main.sbdailyprice AS sbdailyprice
  WHERE
    sbdailyprice.sbdpdate <= '2023-04-04' AND sbdailyprice.sbdpdate >= '2023-04-01'
  GROUP BY
    sbdailyprice.sbdptickerid
), _t0 AS (
  SELECT
    _table_alias_1.agg_0 - _table_alias_1.agg_1 AS ordering_2,
    _table_alias_1.agg_0 - _table_alias_1.agg_1 AS price_change,
    _table_alias_0.symbol AS symbol
  FROM _table_alias_0 AS _table_alias_0
  LEFT JOIN _table_alias_1 AS _table_alias_1
    ON _table_alias_0._id = _table_alias_1.ticker_id
  ORDER BY
    ordering_2 DESC
  LIMIT 3
)
SELECT
  _t0.symbol AS symbol,
  _t0.price_change AS price_change
FROM _t0 AS _t0
ORDER BY
  _t0.ordering_2 DESC
