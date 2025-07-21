WITH _s1 AS (
  SELECT
    (
      100.0 * (
        COALESCE(SUM(sbtxamount), 0) - COALESCE(SUM(sbtxtax + sbtxcommission), 0)
      )
    ) / COALESCE(SUM(sbtxamount), 0) AS SPM,
    sbtxtickerid AS sbTxTickerId
  FROM main.sbTransaction
  WHERE
    sbtxdatetime >= DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL '-1' MONTH)
    AND sbtxtype = 'sell'
  GROUP BY
    sbtxtickerid
)
SELECT
  sbTicker.sbtickersymbol AS symbol,
  _s1.SPM
FROM main.sbTicker AS sbTicker
JOIN _s1 AS _s1
  ON _s1.sbTxTickerId = sbTicker.sbtickerid
ORDER BY
  sbTicker.sbtickersymbol
