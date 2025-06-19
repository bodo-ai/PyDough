WITH _s1 AS (
  SELECT
    COUNT(*) AS agg_0,
    sbtxtickerid AS ticker_id
  FROM main.sbtransaction
  WHERE
    sbtxdatetime >= DATE_TRUNC('DAY', DATE_ADD(CURRENT_TIMESTAMP(), -10, 'DAY'))
    AND sbtxtype = 'buy'
  GROUP BY
    sbtxtickerid
)
SELECT
  sbticker.sbtickersymbol AS symbol,
  COALESCE(_s1.agg_0, 0) AS tx_count
FROM main.sbticker AS sbticker
LEFT JOIN _s1 AS _s1
  ON _s1.ticker_id = sbticker.sbtickerid
ORDER BY
  tx_count DESC
LIMIT 2
