WITH _table_alias_0 AS (
  SELECT
    sbtransaction.sbtxdatetime AS date_time,
    sbtransaction.sbtxtickerid AS ticker_id
  FROM main.sbtransaction AS sbtransaction
  WHERE
    sbtransaction.sbtxdatetime < DATE(
      'now',
      '-' || CAST(STRFTIME('%w', DATETIME('now')) AS INTEGER) || ' days',
      'start of day'
    )
    AND sbtransaction.sbtxdatetime >= DATE(
      'now',
      '-' || CAST(STRFTIME('%w', DATETIME('now')) AS INTEGER) || ' days',
      'start of day',
      '-56 day'
    )
), _table_alias_1 AS (
  SELECT
    sbticker.sbtickerid AS _id
  FROM main.sbticker AS sbticker
  WHERE
    sbticker.sbtickertype = 'stock'
), _t0 AS (
  SELECT
    COUNT() AS agg_0,
    SUM(CAST(STRFTIME('%w', _table_alias_0.date_time) AS INTEGER) IN (5, 6)) AS agg_1,
    DATE(
      _table_alias_0.date_time,
      '-' || CAST(STRFTIME('%w', DATETIME(_table_alias_0.date_time)) AS INTEGER) || ' days',
      'start of day'
    ) AS week
  FROM _table_alias_0 AS _table_alias_0
  JOIN _table_alias_1 AS _table_alias_1
    ON _table_alias_0.ticker_id = _table_alias_1._id
  GROUP BY
    DATE(
      _table_alias_0.date_time,
      '-' || CAST(STRFTIME('%w', DATETIME(_table_alias_0.date_time)) AS INTEGER) || ' days',
      'start of day'
    )
)
SELECT
  _t0.week AS week,
  COALESCE(_t0.agg_0, 0) AS num_transactions,
  COALESCE(_t0.agg_1, 0) AS weekend_transactions
FROM _t0 AS _t0
