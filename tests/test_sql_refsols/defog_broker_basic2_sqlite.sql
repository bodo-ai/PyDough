SELECT
  sbtxtype AS transaction_type,
  COUNT(DISTINCT sbtxcustid) AS num_customers,
  AVG(sbtxshares) AS avg_shares
FROM main.sbtransaction
WHERE
  sbtxdatetime <= '2023-03-31' AND sbtxdatetime >= '2023-01-01'
GROUP BY
  1
ORDER BY
  num_customers DESC,
  sbtxtype
LIMIT 3
