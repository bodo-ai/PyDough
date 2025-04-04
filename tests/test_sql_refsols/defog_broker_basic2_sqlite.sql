WITH _t1 AS (
  SELECT
    AVG(sbtxshares) AS avg_shares,
    COUNT(DISTINCT sbtxcustid) AS num_customers,
    COUNT(DISTINCT sbtxcustid) AS ordering_2,
    sbtxtype AS transaction_type
  FROM main.sbtransaction
  WHERE
    sbtxdatetime <= '2023-03-31' AND sbtxdatetime >= '2023-01-01'
  GROUP BY
    sbtxtype
), _t0 AS (
  SELECT
    avg_shares AS avg_shares,
    num_customers AS num_customers,
    ordering_2 AS ordering_2,
    transaction_type AS transaction_type
  FROM _t1
  ORDER BY
    ordering_2 DESC
  LIMIT 3
)
SELECT
  transaction_type AS transaction_type,
  num_customers AS num_customers,
  avg_shares AS avg_shares
FROM _t0
ORDER BY
  ordering_2 DESC
