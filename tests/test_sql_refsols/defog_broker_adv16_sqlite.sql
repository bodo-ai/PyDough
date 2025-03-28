WITH _table_alias_1 AS (
  SELECT
    SUM(sbtransaction.sbtxamount) AS agg_0,
    SUM(sbtransaction.sbtxtax + sbtransaction.sbtxcommission) AS agg_1,
    sbtransaction.sbtxtickerid AS ticker_id
  FROM main.sbtransaction AS sbtransaction
  WHERE
    sbtransaction.sbtxdatetime >= DATETIME(DATETIME('now'), '-1 month')
    AND sbtransaction.sbtxtype = 'sell'
  GROUP BY
    sbtransaction.sbtxtickerid
)
SELECT
  sbticker.sbtickersymbol AS symbol,
  CAST((
    100.0 * (
      COALESCE(_table_alias_1.agg_0, 0) - COALESCE(_table_alias_1.agg_1, 0)
    )
  ) AS REAL) / COALESCE(_table_alias_1.agg_0, 0) AS SPM
FROM main.sbticker AS sbticker
LEFT JOIN _table_alias_1 AS _table_alias_1
  ON _table_alias_1.ticker_id = sbticker.sbtickerid
WHERE
  NOT CAST((
    100.0 * (
      COALESCE(_table_alias_1.agg_0, 0) - COALESCE(_table_alias_1.agg_1, 0)
    )
  ) AS REAL) / COALESCE(_table_alias_1.agg_0, 0) IS NULL
ORDER BY
  sbticker.sbtickersymbol
