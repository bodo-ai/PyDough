SELECT
  COUNT(DISTINCT o_custkey) AS n
FROM tpch.ORDERS
WHERE
  o_orderpriority = '1-URGENT'
