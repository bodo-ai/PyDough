SELECT
  COUNT(*) AS n1,
  SUM(CASE WHEN c_mktsegment = 'BUILDING' THEN 1 ELSE 0 END) AS n2
FROM tpch.customer
WHERE
  c_acctbal <= 600 AND c_acctbal >= 500
