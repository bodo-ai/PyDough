SELECT
  COUNT(*) AS n1,
  SUM(IFF(c_mktsegment = 'BUILDING', 1, 0)) AS n2
FROM tpch.customer
WHERE
  c_acctbal <= 600 AND c_acctbal >= 500
