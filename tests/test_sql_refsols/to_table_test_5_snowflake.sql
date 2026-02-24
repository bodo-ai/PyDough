SELECT
  c_name AS name
FROM snowflake_sample_data.tpch_sf1.customer
ORDER BY
  1 NULLS FIRST
LIMIT 5
