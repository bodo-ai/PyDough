SELECT
  o_orderkey AS key
FROM mysql.tpch.orders
ORDER BY
  1 NULLS FIRST
LIMIT 5
