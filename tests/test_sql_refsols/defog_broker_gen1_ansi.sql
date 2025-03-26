WITH _table_alias_0 AS (
  SELECT
    sbdailyprice.sbdpclose AS close,
    sbdailyprice.sbdptickerid AS ticker_id
  FROM main.sbdailyprice AS sbdailyprice
  WHERE
    DATEDIFF(CURRENT_TIMESTAMP(), CAST(sbdailyprice.sbdpdate AS DATETIME), DAY) <= 7
), _table_alias_1 AS (
  SELECT
    sbticker.sbtickerid AS _id
  FROM main.sbticker AS sbticker
  WHERE
    sbticker.sbtickersymbol = 'VTI'
)
SELECT
  MIN(_table_alias_0.close) AS lowest_price
FROM _table_alias_0 AS _table_alias_0
JOIN _table_alias_1 AS _table_alias_1
  ON _table_alias_0.ticker_id = _table_alias_1._id
