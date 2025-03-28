WITH _table_alias_1 AS (
  SELECT
    sbdailyprice.sbdptickerid AS ticker_id
  FROM main.sbdailyprice AS sbdailyprice
), _table_alias_0 AS (
  SELECT
    sbticker.sbtickerid AS _id,
    sbticker.sbtickersymbol AS symbol
  FROM main.sbticker AS sbticker
)
SELECT
  _table_alias_0._id AS _id,
  _table_alias_0.symbol AS symbol
FROM _table_alias_0 AS _table_alias_0
WHERE
  NOT EXISTS(
    SELECT
      1 AS "1"
    FROM _table_alias_1 AS _table_alias_1
    WHERE
      _table_alias_0._id = _table_alias_1.ticker_id
  )
