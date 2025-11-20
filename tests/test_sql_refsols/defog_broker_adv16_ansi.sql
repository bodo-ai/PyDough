WITH _s1 AS (
  SELECT
    sbtxtickerid,
    SUM(sbtxtax + sbtxcommission) AS sum_expr2,
    SUM(sbtxamount) AS sum_sbtxamount
  FROM main.sbtransaction
  WHERE
    sbtxdatetime >= DATE_SUB(CURRENT_TIMESTAMP(), 1, MONTH) AND sbtxtype = 'sell'
  GROUP BY
    1
)
SELECT
  sbticker.sbtickersymbol AS symbol,
  (
    100.0 * (
      COALESCE(_s1.sum_sbtxamount, 0) - COALESCE(_s1.sum_expr2, 0)
    )
  ) / COALESCE(_s1.sum_sbtxamount, 0) AS SPM
FROM main.sbticker AS sbticker
JOIN _s1 AS _s1
  ON _s1.sbtxtickerid = sbticker.sbtickerid
ORDER BY
  1
