WITH _t0 AS (
  SELECT
    AVG(sbtxshares) AS avg_sbtxshares,
    COUNT(DISTINCT sbtxcustid) AS ndistinct_sbtxcustid,
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
  ndistinct_sbtxcustid AS num_customers,
  avg_sbtxshares AS avg_shares
FROM _t0
ORDER BY
  ndistinct_sbtxcustid DESC
LIMIT 3
