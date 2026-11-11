SELECT
  o_orderkey AS key
FROM snowflake_sample_data.tpch_sf1.orders
ORDER BY
  1 NULLS FIRST
LIMIT 5
