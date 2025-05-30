SELECT
  o_orderkey AS key
FROM tpch.orders
ORDER BY
  key
LIMIT 5
