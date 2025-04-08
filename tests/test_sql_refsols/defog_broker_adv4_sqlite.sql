WITH _s1 AS (
  SELECT
    MAX(sbdphigh) AS agg_0,
    MIN(sbdplow) AS agg_1,
    sbdptickerid AS ticker_id
  FROM main.sbdailyprice
  WHERE
    sbdpdate <= '2023-04-04' AND sbdpdate >= '2023-04-01'
  GROUP BY
    sbdptickerid
)
SELECT
  sbticker.sbtickersymbol AS symbol,
  _s1.agg_0 - _s1.agg_1 AS price_change
FROM main.sbticker AS sbticker
LEFT JOIN _s1 AS _s1
  ON _s1.ticker_id = sbticker.sbtickerid
ORDER BY
  price_change DESC
LIMIT 3
