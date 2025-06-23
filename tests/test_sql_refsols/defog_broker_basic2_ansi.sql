WITH _t0 AS (
  SELECT
    AVG(sbtxshares) AS avg_shares,
    COUNT(DISTINCT sbtxcustid) AS num_customers,
    sbtxtype
  FROM main.sbtransaction
  WHERE
    sbtxdatetime <= CAST('2023-03-31' AS DATE)
    AND sbtxdatetime >= CAST('2023-01-01' AS DATE)
  GROUP BY
    sbtxtype
)
SELECT
  sbtxtype AS transaction_type,
  num_customers,
  avg_shares
FROM _t0
ORDER BY
  num_customers DESC
LIMIT 3
