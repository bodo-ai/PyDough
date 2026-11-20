SELECT
  supplier.s_name AS name,
  region.r_name AS region_name,
  supplier.s_acctbal - LAG(supplier.s_acctbal, 1) OVER (PARTITION BY nation.n_regionkey ORDER BY supplier.s_acctbal NULLS LAST) AS acctbal_delta
FROM tpch_s3_pq.tpch_sf1.region AS region
JOIN tpch_s3_pq.tpch_sf1.nation AS nation
  ON nation.n_regionkey = region.r_regionkey
JOIN tpch_s3_pq.tpch_sf1.supplier AS supplier
  ON nation.n_nationkey = supplier.s_nationkey
ORDER BY
  3 DESC
LIMIT 5
