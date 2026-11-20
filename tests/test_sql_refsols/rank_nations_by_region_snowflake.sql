SELECT
  nation.n_name AS name,
  RANK() OVER (ORDER BY region.r_name) AS rank
FROM snowflake_sample_data.tpch_sf1.nation AS nation
JOIN snowflake_sample_data.tpch_sf1.region AS region
  ON nation.n_regionkey = region.r_regionkey
