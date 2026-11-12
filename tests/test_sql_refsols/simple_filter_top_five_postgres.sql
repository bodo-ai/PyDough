SELECT
  o_orderkey AS key
FROM tpch.orders
WHERE
  o_totalprice < 1000.0
ORDER BY
  1 DESC NULLS LAST
LIMIT 5
