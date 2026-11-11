SELECT
  o_orderkey AS key
FROM main.orders
ORDER BY
  1 NULLS FIRST
LIMIT 5
