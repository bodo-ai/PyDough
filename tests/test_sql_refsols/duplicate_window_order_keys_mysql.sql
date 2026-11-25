SELECT
  o_orderkey AS oid,
  o_custkey AS c,
  LAG(o_orderkey, 1) OVER (ORDER BY CASE WHEN o_orderkey IS NULL THEN 1 ELSE 0 END, o_orderkey) AS prev_oid
FROM tpch.ORDERS
WHERE
  o_custkey <= 2
ORDER BY
  1
