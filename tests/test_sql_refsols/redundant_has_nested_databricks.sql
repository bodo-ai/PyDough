SELECT
  COUNT(*) AS n
FROM tpch_s3_pq.tpch_sf1.supplier AS supplier
JOIN tpch_s3_pq.tpch_sf1.nation AS nation
  ON nation.n_nationkey = supplier.s_nationkey
JOIN tpch_s3_pq.tpch_sf1.region AS region
  ON nation.n_regionkey = region.r_regionkey AND region.r_name = 'AFRICA'
