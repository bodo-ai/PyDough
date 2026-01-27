SELECT
  COUNT(*) AS n1,
  SUM(CASE WHEN c_mktsegment = 'BUILDING' THEN 1 ELSE 0 END) AS n2,
  SUM(CASE WHEN c_acctbal <= 600 AND c_acctbal >= 500 THEN 1 ELSE 0 END) AS n3,
  SUM(CASE WHEN c_phone LIKE '11%' THEN 1 ELSE 0 END) AS n4,
  SUM(CASE WHEN c_mktsegment = 'BUILDING' AND c_phone LIKE '11%' THEN 1 ELSE 0 END) AS n5,
  SUM(
    CASE
      WHEN c_acctbal <= 600
      AND c_acctbal >= 500
      AND c_mktsegment = 'BUILDING'
      AND c_phone LIKE '11%'
      THEN 1
      ELSE 0
    END
  ) AS n6
FROM tpch.CUSTOMER
