SELECT
  o_orderkey AS oid,
  o_totalprice AS amount
FROM tpch.orders
ORDER BY
  2 DESC,
  1
LIMIT 5
