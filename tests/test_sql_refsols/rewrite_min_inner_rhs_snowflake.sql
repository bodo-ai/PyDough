SELECT
  MIN(o_custkey) AS min_k
FROM tpch.orders
WHERE
  YEAR(CAST(o_orderdate AS TIMESTAMP)) = 1994 AND o_orderpriority = '1-URGENT'
