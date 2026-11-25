SELECT
  o_orderkey AS oid,
  o_custkey AS c1,
  o_custkey AS c2
FROM tpch_s3_pq.tpch_sf1.orders
WHERE
  o_custkey <= 2
ORDER BY
  2,
  1
