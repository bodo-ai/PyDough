WITH _s1 AS (
  SELECT
    SUM(sbtxtax + sbtxcommission) AS sum_expr_2,
    SUM(sbtxamount) AS sum_sbtxamount,
    sbtxtickerid
  FROM main.sbtransaction
  WHERE
    sbtxdatetime >= DATETIME('now', '-1 month') AND sbtxtype = 'sell'
  GROUP BY
    3
)
SELECT
  sbticker.sbtickersymbol AS symbol,
  CAST((
    100.0 * (
      COALESCE(_s1.sum_sbtxamount, 0) - COALESCE(_s1.sum_expr_2, 0)
    )
  ) AS REAL) / COALESCE(_s1.sum_sbtxamount, 0) AS SPM
FROM main.sbticker AS sbticker
JOIN _s1 AS _s1
  ON _s1.sbtxtickerid = sbticker.sbtickerid
ORDER BY
  1
