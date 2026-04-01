SELECT
  COUNT(DISTINCT o_custkey) AS n
FROM TPCH.ORDERS
WHERE
  o_orderpriority = '1-URGENT'
