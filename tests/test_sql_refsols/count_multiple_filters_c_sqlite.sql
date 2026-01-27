SELECT
  COUNT(*) AS n1,
  SUM(IIF(c_mktsegment = 'BUILDING', 1, 0)) AS n2,
  SUM(IIF(c_acctbal <= 600 AND c_acctbal >= 500, 1, 0)) AS n3,
  SUM(IIF(c_phone LIKE '11%', 1, 0)) AS n4,
  SUM(IIF(c_mktsegment = 'BUILDING' AND c_phone LIKE '11%', 1, 0)) AS n5,
  SUM(
    IIF(
      c_acctbal <= 600
      AND c_acctbal >= 500
      AND c_mktsegment = 'BUILDING'
      AND c_phone LIKE '11%',
      1,
      0
    )
  ) AS n6
FROM tpch.customer
