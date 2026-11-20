SELECT
  COUNT(DISTINCT c_nationkey) AS n
FROM main.customer
WHERE
  c_acctbal < -975 AND c_mktsegment = 'AUTOMOBILE'
