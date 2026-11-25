SELECT
  o_orderkey AS oid,
  o_totalprice AS amount
FROM mysql.tpch.orders
ORDER BY
  2 DESC,
  1 NULLS FIRST
LIMIT 5
