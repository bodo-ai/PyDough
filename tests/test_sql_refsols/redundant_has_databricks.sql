SELECT
  COUNT(*) AS n
FROM tpch_s3_pq.tpch_sf1.customer AS customer
JOIN tpch_s3_pq.tpch_sf1.nation AS nation
  ON customer.c_nationkey = nation.n_nationkey
JOIN tpch_s3_pq.tpch_sf1.region AS region
  ON nation.n_regionkey = region.r_regionkey AND region.r_name = 'ASIA'
