SELECT
  COUNT(*) AS n
FROM snowflake_sample_data.tpch_sf1.customer AS customer
JOIN snowflake_sample_data.tpch_sf1.nation AS nation
  ON customer.c_nationkey = nation.n_nationkey
JOIN snowflake_sample_data.tpch_sf1.region AS region
  ON nation.n_regionkey = region.r_regionkey AND region.r_name = 'ASIA'
