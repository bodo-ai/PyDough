WITH "_S1" AS (
  SELECT
    sbtxtickerid AS SBTXTICKERID,
    SUM(sbtxtax + sbtxcommission) AS SUM_EXPR,
    SUM(sbtxamount) AS SUM_SBTXAMOUNT
  FROM MAIN.SBTRANSACTION
  WHERE
    sbtxdatetime >= (
      SYS_EXTRACT_UTC(SYSTIMESTAMP) + NUMTOYMINTERVAL(1, 'month')
    )
    AND sbtxtype = 'sell'
  GROUP BY
    sbtxtickerid
)
SELECT
  SBTICKER.sbtickersymbol AS symbol,
  (
    100.0 * (
      COALESCE("_S1".SUM_SBTXAMOUNT, 0) - COALESCE("_S1".SUM_EXPR, 0)
    )
  ) / NULLIF("_S1".SUM_SBTXAMOUNT, 0) AS SPM
FROM MAIN.SBTICKER SBTICKER
JOIN "_S1" "_S1"
  ON SBTICKER.sbtickerid = "_S1".SBTXTICKERID
ORDER BY
  1 NULLS FIRST
