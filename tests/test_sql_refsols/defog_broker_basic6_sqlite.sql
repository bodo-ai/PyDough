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
    sbdailyprice.sbdpdate >= '2023-04-01'
), _table_alias_1 AS (
  SELECT
    _t0.ticker_id AS ticker_id
  FROM _t0 AS _t0
), _u_0 AS (
  SELECT
    _table_alias_1.ticker_id AS _u_1
  FROM _table_alias_1 AS _table_alias_1
  GROUP BY
    _table_alias_1.ticker_id
)
SELECT
  _table_alias_0._id AS _id
FROM _table_alias_0 AS _table_alias_0
LEFT JOIN _u_0 AS _u_0
  ON _table_alias_0._id = _u_0._u_1
WHERE
  NOT _u_0._u_1 IS NULL
