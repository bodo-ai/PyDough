WITH _T0 AS (
  SELECT
    SUM(sbtxamount) AS AGG_0,
    SUM(sbtxtax + sbtxcommission) AS AGG_1,
    sbtxtickerid AS TICKER_ID
  FROM MAIN.SBTRANSACTION
  WHERE
    sbtxdatetime >= DATEADD(MONTH, -1, CURRENT_TIMESTAMP()) AND sbtxtype = 'sell'
  GROUP BY
    sbtxtickerid
)
SELECT
  SBTICKER.sbtickersymbol AS symbol,
  (
    100.0 * (
      COALESCE(_T0.AGG_0, 0) - COALESCE(_T0.AGG_1, 0)
    )
  ) / COALESCE(_T0.AGG_0, 0) AS SPM
FROM MAIN.SBTICKER AS SBTICKER
JOIN _T0 AS _T0
  ON SBTICKER.sbtickerid = _T0.TICKER_ID
ORDER BY
  SYMBOL NULLS FIRST
