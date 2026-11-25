SELECT
  o_orderkey AS oid,
  o_totalprice AS amount
FROM main.orders
ORDER BY
  2 DESC,
  1 NULLS FIRST
LIMIT 5
