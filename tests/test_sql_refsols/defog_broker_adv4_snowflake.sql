WITH _S1 AS (
  SELECT
    MAX(sbdphigh) AS AGG_0,
    MIN(sbdplow) AS AGG_1,
    sbdptickerid AS TICKER_ID
  FROM MAIN.SBDAILYPRICE
  WHERE
    sbdpdate <= CAST('2023-04-04' AS DATE) AND sbdpdate >= CAST('2023-04-01' AS DATE)
  GROUP BY
    sbdptickerid
)
SELECT
  SBTICKER.sbtickersymbol AS symbol,
  _S1.AGG_0 - _S1.AGG_1 AS price_change
FROM MAIN.SBTICKER AS SBTICKER
LEFT JOIN _S1 AS _S1
  ON SBTICKER.sbtickerid = _S1.TICKER_ID
ORDER BY
  PRICE_CHANGE DESC NULLS LAST
LIMIT 3
