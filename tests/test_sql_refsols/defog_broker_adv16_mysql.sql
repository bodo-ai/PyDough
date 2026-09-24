WITH _s1 AS (
  SELECT
    sbTxTickerId,
    SUM(sbTxTax + sbTxCommission) AS sum_expr,
    SUM(sbTxAmount) AS sum_sbTxAmount
  FROM broker.sbTransaction
  WHERE
    sbTxDateTime >= DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL '1' MONTH)
    AND sbTxType = 'sell'
  GROUP BY
    1
)
SELECT
  sbTicker.sbTickerSymbol COLLATE utf8mb4_bin AS symbol,
  (
    100.0 * (
      COALESCE(_s1.sum_sbTxAmount, 0) - COALESCE(_s1.sum_expr, 0)
    )
  ) / NULLIF(_s1.sum_sbTxAmount, 0) AS SPM
FROM broker.sbTicker AS sbTicker
JOIN _s1 AS _s1
  ON _s1.sbTxTickerId = sbTicker.sbTickerId
ORDER BY
  1
