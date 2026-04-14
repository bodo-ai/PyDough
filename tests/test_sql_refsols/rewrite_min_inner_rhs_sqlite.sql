SELECT
  MIN(o_custkey) AS min_k,
  MAX(o_custkey) AS max_k,
  COUNT(*) AS n
FROM tpch.orders
WHERE
  CAST(STRFTIME('%Y', o_orderdate) AS INTEGER) = 1994
  AND o_orderpriority = '1-URGENT'
