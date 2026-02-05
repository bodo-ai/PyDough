WITH _S1 AS (
  SELECT
    sbtxtickerid AS SBTXTICKERID,
    SUM(sbtxtax + sbtxcommission) AS SUM_EXPR,
    SUM(sbtxamount) AS SUM_SBTXAMOUNT
  FROM MAIN.SBTRANSACTION
  WHERE
    sbtxdatetime >= DATE_SUB(CURRENT_TIMESTAMP, 1, MONTH) AND sbtxtype = 'sell'
  GROUP BY
    sbtxtickerid
)
SELECT
  SBTICKER.sbtickersymbol AS symbol,
  (
    100.0 * (
      NVL(_S1.SUM_SBTXAMOUNT, 0) - NVL(_S1.SUM_EXPR, 0)
    )
  ) / NVL(_S1.SUM_SBTXAMOUNT, 0) AS SPM
FROM MAIN.SBTICKER SBTICKER
JOIN _S1 _S1
  ON SBTICKER.sbtickerid = _S1.SBTXTICKERID
ORDER BY
  1 NULLS FIRST
