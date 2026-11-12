SELECT
  COUNT(DISTINCT o_custkey) AS n
FROM tpch.orders
WHERE
  o_orderpriority = '1-URGENT'
