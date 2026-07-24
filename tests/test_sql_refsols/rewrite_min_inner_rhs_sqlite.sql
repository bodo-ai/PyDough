SELECT
  MIN(o_custkey) AS min_k
FROM tpch.orders
WHERE
  CAST(STRFTIME('%Y', o_orderdate) AS INTEGER) = 1994
  AND o_orderpriority = '1-URGENT'
