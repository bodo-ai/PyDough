WITH _s1 AS (
  SELECT
    MAX(sbdphigh) AS max_sbDpHigh,
    MIN(sbdplow) AS min_sbDpLow,
    sbdptickerid AS sbDpTickerId
  FROM main.sbDailyPrice
  WHERE
    sbdpdate <= CAST('2023-04-04' AS DATE) AND sbdpdate >= CAST('2023-04-01' AS DATE)
  GROUP BY
    sbdptickerid
)
SELECT
  sbTicker.sbtickersymbol AS symbol,
  _s1.max_sbDpHigh - _s1.min_sbDpLow AS price_change
FROM main.sbTicker AS sbTicker
LEFT JOIN _s1 AS _s1
  ON _s1.sbDpTickerId = sbTicker.sbtickerid
ORDER BY
  _s1.max_sbDpHigh - _s1.min_sbDpLow DESC
LIMIT 3
