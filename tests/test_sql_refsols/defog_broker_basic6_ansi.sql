WITH _table_alias_0 AS (
  SELECT
    sbticker.sbtickerid AS _id
  FROM main.sbticker AS sbticker
), _t0 AS (
  SELECT
    sbdailyprice.sbdpdate AS date,
    sbdailyprice.sbdptickerid AS ticker_id
  FROM main.sbdailyprice AS sbdailyprice
  WHERE
    sbdailyprice.sbdpdate >= CAST('2023-04-01' AS DATE)
), _table_alias_1 AS (
  SELECT
    _t0.ticker_id AS ticker_id
  FROM _t0 AS _t0
)
SELECT
  _table_alias_0._id AS _id
FROM _table_alias_0 AS _table_alias_0
JOIN _table_alias_1 AS _table_alias_1
  ON _table_alias_0._id = _table_alias_1.ticker_id
