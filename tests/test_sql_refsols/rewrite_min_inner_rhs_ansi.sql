SELECT
  MIN(o_custkey) AS min_k,
  MAX(o_custkey) AS max_k,
  COUNT(*) AS n
FROM tpch.orders
WHERE
  EXTRACT(YEAR FROM CAST(o_orderdate AS DATETIME)) = 1994
  AND o_orderpriority = '1-URGENT'
