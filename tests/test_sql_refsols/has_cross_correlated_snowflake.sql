SELECT
  COUNT(DISTINCT customer.c_custkey) AS n
FROM snowflake_sample_data.tpch_sf1.customer AS customer
JOIN snowflake_sample_data.tpch_sf1.supplier AS supplier
  ON customer.c_nationkey = supplier.s_nationkey
