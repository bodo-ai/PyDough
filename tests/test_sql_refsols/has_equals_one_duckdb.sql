SELECT
  COUNT(DISTINCT o_custkey) AS n
FROM main.orders
WHERE
  o_orderpriority = '1-URGENT'
