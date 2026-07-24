SELECT
  COUNT(DISTINCT o_custkey) AS n
FROM tpch.orders
WHERE
  EXTRACT(YEAR FROM CAST(o_orderdate AS TIMESTAMP)) = 1994
  AND o_orderpriority = '1-URGENT'
