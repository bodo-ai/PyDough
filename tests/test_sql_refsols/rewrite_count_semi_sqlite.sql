SELECT
  COUNT(DISTINCT o_custkey) AS n
FROM tpch.orders
WHERE
  CAST(STRFTIME('%Y', o_orderdate) AS INTEGER) = 1994
  AND o_orderpriority = '1-URGENT'
