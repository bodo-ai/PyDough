WITH _t1 AS (
  SELECT
    SUM(sbtxtax + sbtxcommission) AS sum_expr_2,
    SUM(sbtxamount) AS sum_sbtxamount,
    sbtxtickerid AS ticker_id
  FROM main.sbtransaction
  WHERE
    sbtxdatetime >= DATETIME('now', '-1 month') AND sbtxtype = 'sell'
  GROUP BY
    sbtxtickerid
)
SELECT
  sbticker.sbtickersymbol AS symbol,
  CAST((
    100.0 * (
      COALESCE(_t1.sum_sbtxamount, 0) - COALESCE(_t1.sum_expr_2, 0)
    )
  ) AS REAL) / COALESCE(_t1.sum_sbtxamount, 0) AS SPM
FROM main.sbticker AS sbticker
JOIN _t1 AS _t1
  ON _t1.ticker_id = sbticker.sbtickerid
ORDER BY
  sbticker.sbtickersymbol
