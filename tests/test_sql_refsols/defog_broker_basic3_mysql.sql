WITH _s1 AS (
  SELECT
    sbTxTickerId,
    COUNT(*) AS n_rows,
    SUM(sbTxAmount) AS sum_sbTxAmount
  FROM broker.sbTransaction
  GROUP BY
    1
)
SELECT
  sbTicker.sbTickerSymbol AS symbol,
  COALESCE(_s1.n_rows, 0) AS num_transactions,
  COALESCE(_s1.sum_sbTxAmount, 0) AS total_amount
FROM broker.sbTicker AS sbTicker
LEFT JOIN _s1 AS _s1
  ON _s1.sbTxTickerId = sbTicker.sbTickerId
ORDER BY
  3 DESC
LIMIT 10
