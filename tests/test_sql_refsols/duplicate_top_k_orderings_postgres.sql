SELECT
  o_orderkey AS oid,
  o_totalprice AS amount
FROM tpch.orders
ORDER BY
  2 DESC NULLS LAST,
  1 NULLS FIRST
LIMIT 5
