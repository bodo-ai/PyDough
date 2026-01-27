SELECT
  COUNT(*) AS n1,
  SUM(IFF(c_mktsegment = 'BUILDING', 1, 0)) AS n2,
  SUM(IFF(c_acctbal <= 600 AND c_acctbal >= 500, 1, 0)) AS n3,
  SUM(IFF(STARTSWITH(c_phone, '11'), 1, 0)) AS n4,
  SUM(IFF(STARTSWITH(c_phone, '11') AND c_mktsegment = 'BUILDING', 1, 0)) AS n5,
  SUM(
    IFF(
      STARTSWITH(c_phone, '11')
      AND c_acctbal <= 600
      AND c_acctbal >= 500
      AND c_mktsegment = 'BUILDING',
      1,
      0
    )
  ) AS n6
FROM tpch.customer
