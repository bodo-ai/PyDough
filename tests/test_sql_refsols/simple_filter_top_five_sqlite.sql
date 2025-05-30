SELECT
  o_orderkey AS key
FROM tpch.orders
WHERE
  o_totalprice < 1000.0
ORDER BY
  key DESC
LIMIT 5
