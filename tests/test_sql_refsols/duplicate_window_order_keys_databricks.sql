SELECT
  o_orderkey AS oid,
  o_custkey AS c,
  LAG(o_orderkey, 1) OVER (ORDER BY o_orderkey NULLS LAST) AS prev_oid
FROM tpch_s3_pq.tpch_sf1.orders
WHERE
  o_custkey <= 2
ORDER BY
  1
