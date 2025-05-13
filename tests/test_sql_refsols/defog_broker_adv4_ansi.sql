WITH _s0 AS (
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
  sbticker.sbtickersymbol AS symbol,
  _s0.agg_0 - _s0.agg_1 AS price_change
FROM main.sbticker AS sbticker
LEFT JOIN _s0 AS _s0
  ON _s0.ticker_id = sbticker.sbtickerid
JOIN main.sbticker AS sbticker_2
  ON _s0.ticker_id = sbticker_2.sbtickerid
ORDER BY
  price_change DESC
LIMIT 3
