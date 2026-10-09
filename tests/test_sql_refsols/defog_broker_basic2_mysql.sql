SELECT
  sbTxType COLLATE utf8mb4_bin AS transaction_type,
  COUNT(DISTINCT sbTxCustId) AS num_customers,
  AVG(sbTxShares) AS avg_shares
FROM broker.sbTransaction
WHERE
  sbTxDateTime <= CAST('2023-03-31' AS DATE)
  AND sbTxDateTime >= CAST('2023-01-01' AS DATE)
GROUP BY
  1
ORDER BY
  2 DESC,
  1
LIMIT 3
