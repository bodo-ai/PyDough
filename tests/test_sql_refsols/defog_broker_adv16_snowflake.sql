WITH _s1 AS (
  SELECT
    sbtxtickerid,
    SUM(sbtxtax + sbtxcommission) AS sum_expr,
    SUM(sbtxamount) AS sum_sbtxamount
  FROM main.sbtransaction
  WHERE
    sbtxdatetime >= DATEADD(MONTH, -1, CURRENT_TIMESTAMP()) AND sbtxtype = 'sell'
  GROUP BY
    1
)
SELECT
  sbticker.sbtickersymbol AS symbol,
  (
    100.0 * (
      COALESCE(_s1.sum_sbtxamount, 0) - COALESCE(_s1.sum_expr, 0)
    )
  ) / COALESCE(_s1.sum_sbtxamount, 0) AS SPM
FROM main.sbticker AS sbticker
JOIN _s1 AS _s1
  ON _s1.sbtxtickerid = sbticker.sbtickerid
ORDER BY
  1 NULLS FIRST
