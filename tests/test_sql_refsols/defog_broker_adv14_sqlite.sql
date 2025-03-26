WITH _table_alias_0 AS (
  SELECT
    sbdailyprice.sbdpclose AS close,
    sbdailyprice.sbdptickerid AS ticker_id
  FROM main.sbdailyprice AS sbdailyprice
  WHERE
    CAST((
      JULIANDAY(DATE(DATETIME('now'), 'start of day')) - JULIANDAY(DATE(sbdailyprice.sbdpdate, 'start of day'))
    ) AS INTEGER) <= 7
), _table_alias_1 AS (
  SELECT
    sbticker.sbtickerid AS _id,
    sbticker.sbtickertype AS ticker_type
  FROM main.sbticker AS sbticker
)
SELECT
  _table_alias_1.ticker_type AS ticker_type,
  AVG(_table_alias_0.close) AS ACP
FROM _table_alias_0 AS _table_alias_0
LEFT JOIN _table_alias_1 AS _table_alias_1
  ON _table_alias_0.ticker_id = _table_alias_1._id
GROUP BY
  _table_alias_1.ticker_type
