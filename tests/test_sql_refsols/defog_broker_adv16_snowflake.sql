WITH _S1 AS (
  SELECT
    (
      100.0 * (
        COALESCE(SUM(sbtxamount), 0) - COALESCE(SUM(sbtxtax + sbtxcommission), 0)
      )
    ) / COALESCE(SUM(sbtxamount), 0) AS SPM,
    sbtxtickerid AS SBTXTICKERID
  FROM MAIN.SBTRANSACTION
  WHERE
    sbtxdatetime >= DATEADD(MONTH, -1, CURRENT_TIMESTAMP()) AND sbtxtype = 'sell'
  GROUP BY
    sbtxtickerid
)
SELECT
  SBTICKER.sbtickersymbol AS symbol,
  _S1.SPM
FROM MAIN.SBTICKER AS SBTICKER
JOIN _S1 AS _S1
  ON SBTICKER.sbtickerid = _S1.SBTXTICKERID
ORDER BY
  SBTICKER.sbtickersymbol NULLS FIRST
