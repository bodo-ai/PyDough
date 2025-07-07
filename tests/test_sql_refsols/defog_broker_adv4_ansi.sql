WITH _s1 AS (
  SELECT
    MAX(sbdphigh) AS max_sbdphigh,
    MIN(sbdplow) AS min_sbdplow,
    sbdptickerid
  FROM main.sbdailyprice
  WHERE
    sbdpdate <= CAST('2023-04-04' AS DATE) AND sbdpdate >= CAST('2023-04-01' AS DATE)
  GROUP BY
    sbdptickerid
)
SELECT
  sbticker.sbtickersymbol AS symbol,
  _s1.max_sbdphigh - _s1.min_sbdplow AS price_change
FROM main.sbticker AS sbticker
LEFT JOIN _s1 AS _s1
  ON _s1.sbdptickerid = sbticker.sbtickerid
ORDER BY
  price_change DESC
LIMIT 3
