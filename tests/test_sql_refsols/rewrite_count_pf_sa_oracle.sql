SELECT
  COUNT(DISTINCT c_nationkey) AS n
FROM TPCH.CUSTOMER
WHERE
  c_acctbal < -975 AND c_mktsegment = 'AUTOMOBILE'
