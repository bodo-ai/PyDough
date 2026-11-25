SELECT
  o_orderkey AS oid,
  o_custkey AS c1,
  o_custkey AS c2
FROM tpch.orders
WHERE
  o_custkey <= 2
ORDER BY
  2,
  1
