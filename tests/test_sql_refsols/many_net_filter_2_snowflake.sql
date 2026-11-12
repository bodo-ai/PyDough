SELECT
  COUNT(*) AS n
FROM snowflake_sample_data.tpch_sf1.supplier AS supplier
JOIN snowflake_sample_data.tpch_sf1.customer AS customer
  ON customer.c_custkey = supplier.s_suppkey
  AND customer.c_nationkey = supplier.s_nationkey
WHERE
  supplier.s_nationkey = 2
