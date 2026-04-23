SELECT
  MIN(o_custkey) AS min_k
FROM tpch.ORDERS
WHERE
  EXTRACT(YEAR FROM CAST(o_orderdate AS DATETIME)) = 1994
  AND o_orderpriority = '1-URGENT'
