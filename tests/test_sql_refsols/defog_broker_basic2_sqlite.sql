WITH _t0 AS (
  SELECT
    AVG(sbtxshares) AS avg_shares,
    COUNT(DISTINCT sbtxcustid) AS num_customers,
    sbtxtype AS transaction_type
  FROM main.sbtransaction
  WHERE
    sbtxdatetime <= '2023-03-31' AND sbtxdatetime >= '2023-01-01'
  GROUP BY
    sbtxtype
)
SELECT
  transaction_type,
  num_customers,
  avg_shares
FROM _t0
ORDER BY
  num_customers DESC
LIMIT 3
