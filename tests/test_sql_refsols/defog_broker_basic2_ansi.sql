WITH _t1 AS (
  SELECT
    AVG(sbtxshares) AS agg_0,
    COUNT(DISTINCT sbtxcustid) AS agg_1,
    sbtxtype AS transaction_type
  FROM main.sbtransaction
  WHERE
    sbtxdatetime <= CAST('2023-03-31' AS DATE)
    AND sbtxdatetime >= CAST('2023-01-01' AS DATE)
  GROUP BY
    sbtxtype
)
SELECT
  transaction_type,
  COALESCE(agg_1, 0) AS num_customers,
  agg_0 AS avg_shares
FROM _t1
ORDER BY
  num_customers DESC
LIMIT 3
