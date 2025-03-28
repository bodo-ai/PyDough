WITH _t0 AS (
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
), _table_alias_0 AS (
  SELECT
    sbticker.sbtickerid AS _id
  FROM main.sbticker AS sbticker
)
SELECT
  _table_alias_0._id AS _id
FROM _table_alias_0 AS _table_alias_0
WHERE
  EXISTS(
    SELECT
      1 AS "1"
    FROM _table_alias_1 AS _table_alias_1
    WHERE
      _table_alias_0._id = _table_alias_1.ticker_id
  )
