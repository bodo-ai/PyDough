SELECT
  o_orderkey AS key
FROM tpch.orders
ORDER BY
  1 NULLS FIRST
LIMIT 5
