SELECT
  COUNT(DISTINCT o_custkey) AS n
FROM mysql.tpch.orders
WHERE
  YEAR(CAST(o_orderdate AS TIMESTAMP)) = 1994 AND o_orderpriority = '1-URGENT'
