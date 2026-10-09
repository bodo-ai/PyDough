WITH _s1 AS (
  SELECT
    sbDpTickerId,
    MAX(sbDpHigh) AS max_sbDpHigh,
    MIN(sbDpLow) AS min_sbDpLow
  FROM broker.sbDailyPrice
  WHERE
    sbDpDate <= CAST('2023-04-04' AS DATE) AND sbDpDate >= CAST('2023-04-01' AS DATE)
  GROUP BY
    1
)
SELECT
  sbTicker.sbTickerSymbol AS symbol,
  _s1.max_sbDpHigh - _s1.min_sbDpLow AS price_change
FROM broker.sbTicker AS sbTicker
LEFT JOIN _s1 AS _s1
  ON _s1.sbDpTickerId = sbTicker.sbTickerId
ORDER BY
  2 DESC
LIMIT 3
