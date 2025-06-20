WITH _s3 AS (
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
  _s0.sbtickersymbol AS symbol,
  COALESCE(_s3.agg_0, 0) AS tx_count
FROM main.sbticker AS _s0
LEFT JOIN _s3 AS _s3
  ON _s0.sbtickerid = _s3.ticker_id
ORDER BY
  tx_count DESC
LIMIT 2
