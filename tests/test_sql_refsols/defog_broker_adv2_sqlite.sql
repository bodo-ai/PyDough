WITH _t1 AS (
  SELECT
    COUNT() AS agg_0,
    sbtxtickerid AS ticker_id
  FROM main.sbtransaction
  WHERE
    sbtxdatetime >= DATE(DATETIME('now', '-10 day'), 'start of day')
    AND sbtxtype = 'buy'
  GROUP BY
    sbtxtickerid
)
SELECT
  sbticker.sbtickersymbol AS symbol,
  COALESCE(_t1.agg_0, 0) AS tx_count
FROM main.sbticker AS sbticker
LEFT JOIN _t1 AS _t1
  ON _t1.ticker_id = sbticker.sbtickerid
ORDER BY
  tx_count DESC
LIMIT 2
