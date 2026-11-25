SELECT
  o_orderkey AS oid,
  o_custkey AS c,
  LAG(o_orderkey, 1) OVER (ORDER BY o_orderkey) AS prev_oid
FROM main.orders
WHERE
  o_custkey <= 2
ORDER BY
  1 NULLS FIRST
