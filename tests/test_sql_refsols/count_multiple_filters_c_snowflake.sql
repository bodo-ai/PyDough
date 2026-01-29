SELECT
  COUNT(*) AS n1,
  COUNT_IF(c_mktsegment = 'BUILDING') AS n2,
  COUNT_IF(c_acctbal <= 600 AND c_acctbal >= 500) AS n3,
  COUNT_IF(STARTSWITH(c_phone, '11')) AS n4,
  COUNT_IF(STARTSWITH(c_phone, '11') AND c_mktsegment = 'BUILDING') AS n5,
  COUNT_IF(
    STARTSWITH(c_phone, '11')
    AND c_acctbal <= 600
    AND c_acctbal >= 500
    AND c_mktsegment = 'BUILDING'
  ) AS n6
FROM tpch.customer
