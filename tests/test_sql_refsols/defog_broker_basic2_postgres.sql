SELECT
  sbtxtype AS transaction_type,
  COUNT(DISTINCT sbtxcustid) AS num_customers,
  AVG(CAST(sbtxshares AS DECIMAL)) AS avg_shares
FROM main.sbtransaction
WHERE
  sbtxdatetime <= CAST('2023-03-31' AS DATE)
  AND sbtxdatetime >= CAST('2023-01-01' AS DATE)
GROUP BY
  1
ORDER BY
  2 DESC NULLS LAST,
  1 NULLS FIRST
LIMIT 3
