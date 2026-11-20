SELECT
  COUNT(DISTINCT c_nationkey) AS n
FROM snowflake_sample_data.tpch_sf1.customer
WHERE
  c_acctbal < -975 AND c_mktsegment = 'AUTOMOBILE'
