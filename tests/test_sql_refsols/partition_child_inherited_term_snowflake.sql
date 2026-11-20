SELECT
  region.r_regionkey AS rkey,
  MAX(region.r_name) AS region_name,
  COUNT(*) AS n_nations
FROM snowflake_sample_data.tpch_sf1.region AS region
JOIN snowflake_sample_data.tpch_sf1.nation AS nation
  ON nation.n_regionkey = region.r_regionkey
GROUP BY
  1
ORDER BY
  1 NULLS FIRST
