WITH _table_alias_0 AS (
  SELECT
    sbticker.sbtickerid AS _id,
    sbticker.sbtickersymbol AS symbol
  FROM main.sbticker AS sbticker
), _table_alias_1 AS (
  SELECT
    sbdailyprice.sbdptickerid AS ticker_id
  FROM main.sbdailyprice AS sbdailyprice
), _u_0 AS (
  SELECT
    _table_alias_1.ticker_id AS _u_1
  FROM _table_alias_1 AS _table_alias_1
  GROUP BY
    _table_alias_1.ticker_id
)
SELECT
  _table_alias_0._id AS _id,
  _table_alias_0.symbol AS symbol
FROM _table_alias_0 AS _table_alias_0
LEFT JOIN _u_0 AS _u_0
  ON _table_alias_0._id = _u_0._u_1
WHERE
  _u_0._u_1 IS NULL
