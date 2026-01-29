SELECT
  COUNT(*) AS n1,
  SUM(c_mktsegment = 'BUILDING') AS n2
FROM tpch.customer
WHERE
  c_acctbal <= 600 AND c_acctbal >= 500
