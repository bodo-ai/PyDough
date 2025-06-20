WITH _s3 AS (
  SELECT
    MAX(sbdphigh) AS agg_0,
    MIN(sbdplow) AS agg_1,
    sbdptickerid AS ticker_id
  FROM main.sbdailyprice
  WHERE
    sbdpdate <= CAST('2023-04-04' AS DATE) AND sbdpdate >= CAST('2023-04-01' AS DATE)
  GROUP BY
    sbdptickerid
)
SELECT
  _s0.sbtickersymbol AS symbol,
  _s3.agg_0 - _s3.agg_1 AS price_change
FROM main.sbticker AS _s0
LEFT JOIN _s3 AS _s3
  ON _s0.sbtickerid = _s3.ticker_id
ORDER BY
  price_change DESC
LIMIT 3
