SELECT
  o_custkey AS c1,
  o_orderkey AS oid,
  LAG(o_orderkey, 1) OVER (PARTITION BY o_custkey ORDER BY o_orderkey NULLS LAST) AS prev_oid
FROM tpch.orders
WHERE
  o_custkey <= 2
ORDER BY
  1,
  2
