WITH _s1 AS (
  SELECT
    sbtxtickerid AS sbTxTickerId,
    COUNT(*) AS n_rows,
    SUM(sbtxamount) AS sum_sbTxAmount
  FROM main.sbTransaction
  GROUP BY
    1
)
SELECT
  sbTicker.sbtickersymbol AS symbol,
  COALESCE(_s1.n_rows, 0) AS num_transactions,
  COALESCE(_s1.sum_sbTxAmount, 0) AS total_amount
FROM main.sbTicker AS sbTicker
LEFT JOIN _s1 AS _s1
  ON _s1.sbTxTickerId = sbTicker.sbtickerid
ORDER BY
  3 DESC
LIMIT 10
