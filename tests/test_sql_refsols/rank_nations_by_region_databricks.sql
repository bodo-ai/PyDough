SELECT
  nation.n_name AS name,
  RANK() OVER (ORDER BY region.r_name NULLS LAST) AS rank
FROM tpch_s3_pq.tpch_sf1.nation AS nation
JOIN tpch_s3_pq.tpch_sf1.region AS region
  ON nation.n_regionkey = region.r_regionkey
