SELECT
  c_custkey AS id,
  ROW_NUMBER() OVER (ORDER BY c_acctbal DESC) AS rk
FROM snowflake_sample_data.tpch_sf1.customer
