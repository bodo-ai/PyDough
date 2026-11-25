SELECT
  o_orderkey AS oid,
  o_custkey AS c1,
  o_custkey AS c2
FROM main.orders
WHERE
  o_custkey <= 2
ORDER BY
  2 NULLS FIRST,
  1 NULLS FIRST
