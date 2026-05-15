SELECT
  COUNT(DISTINCT c_nationkey) AS n
FROM tpch.CUSTOMER
WHERE
  c_acctbal < -975 AND c_mktsegment = 'AUTOMOBILE'
