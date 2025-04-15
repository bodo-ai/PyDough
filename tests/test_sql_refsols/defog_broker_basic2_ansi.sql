WITH _t2 AS (
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
), _t0 AS (
  SELECT
    agg_0 AS avg_shares,
    COALESCE(agg_1, 0) AS num_customers,
    COALESCE(agg_1, 0) AS ordering_2,
    transaction_type
  FROM _t2
  ORDER BY
    ordering_2 DESC
  LIMIT 3
)
SELECT
  transaction_type,
  num_customers,
  avg_shares
FROM _t0
ORDER BY
  ordering_2 DESC
