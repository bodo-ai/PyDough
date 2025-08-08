SELECT
  sbtxtype AS transaction_type,
  COUNT(DISTINCT sbtxcustid) AS num_customers,
  AVG(sbtxshares) AS avg_shares
FROM main.sbtransaction
WHERE
  sbtxdatetime <= CAST('2023-03-31' AS DATE)
  AND sbtxdatetime >= CAST('2023-01-01' AS DATE)
GROUP BY
  sbtxtype
ORDER BY
  num_customers DESC NULLS LAST,
  sbtxtype NULLS FIRST
LIMIT 3
