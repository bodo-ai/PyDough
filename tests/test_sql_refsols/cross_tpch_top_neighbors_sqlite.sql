SELECT
  o_custkey AS c,
  o_orderkey AS oid,
  LAG(o_orderkey, 1) OVER (PARTITION BY o_custkey ORDER BY o_orderkey) AS prev_oid,
  LEAD(o_orderkey, 1) OVER (PARTITION BY o_custkey ORDER BY o_orderkey) AS next_oid
FROM tpch.orders
WHERE
  o_custkey <= 2
