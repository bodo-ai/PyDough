WITH _s1 AS (
  SELECT
    SUM(sbtxtax + sbtxcommission) AS sum_expr_2,
    SUM(sbtxamount) AS sum_sbtxamount,
    sbtxtickerid
  FROM main.sbtransaction
  WHERE
    sbtxdatetime >= DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL '-1' MONTH)
    AND sbtxtype = 'sell'
  GROUP BY
    3
)
SELECT
  sbticker.sbtickersymbol COLLATE utf8mb4_bin AS symbol,
  (
    100.0 * (
      COALESCE(_s1.sum_sbtxamount, 0) - COALESCE(_s1.sum_expr_2, 0)
    )
  ) / COALESCE(_s1.sum_sbtxamount, 0) AS SPM
FROM main.sbticker AS sbticker
JOIN _s1 AS _s1
  ON _s1.sbtxtickerid = sbticker.sbtickerid
ORDER BY
  1
