WITH _s1 AS (
  SELECT
    SUM(sbtxtax + sbtxcommission) AS sum_expr_2,
    SUM(sbtxamount) AS sum_sbTxAmount,
    sbtxtickerid AS sbTxTickerId
  FROM main.sbTransaction
  WHERE
    sbtxdatetime >= DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL '1' MONTH)
    AND sbtxtype = 'sell'
  GROUP BY
    3
)
SELECT
  sbTicker.sbtickersymbol COLLATE utf8mb4_bin AS symbol,
  (
    100.0 * (
      COALESCE(_s1.sum_sbTxAmount, 0) - COALESCE(_s1.sum_expr_2, 0)
    )
  ) / COALESCE(_s1.sum_sbTxAmount, 0) AS SPM
FROM main.sbTicker AS sbTicker
JOIN _s1 AS _s1
  ON _s1.sbTxTickerId = sbTicker.sbtickerid
ORDER BY
  1
