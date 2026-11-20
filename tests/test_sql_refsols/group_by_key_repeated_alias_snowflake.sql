SELECT
  c_nationkey AS key,
  c_nationkey AS key_again,
  COUNT(*) AS cnt
FROM snowflake_sample_data.tpch_sf1.customer
GROUP BY
  1
ORDER BY
  1 NULLS FIRST
