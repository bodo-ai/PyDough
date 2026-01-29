SELECT
  COUNT(*) AS n1,
  SUM(c_mktsegment = 'BUILDING') AS n2,
  SUM(c_acctbal <= 600 AND c_acctbal >= 500) AS n3,
  SUM(c_phone LIKE '11%') AS n4,
  SUM(c_mktsegment = 'BUILDING' AND c_phone LIKE '11%') AS n5,
  SUM(
    c_acctbal <= 600
    AND c_acctbal >= 500
    AND c_mktsegment = 'BUILDING'
    AND c_phone LIKE '11%'
  ) AS n6
FROM tpch.customer
