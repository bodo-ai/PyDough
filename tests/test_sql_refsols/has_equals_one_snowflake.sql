SELECT
  COUNT(DISTINCT o_custkey) AS n
FROM snowflake_sample_data.tpch_sf1.orders
WHERE
  o_orderpriority = '1-URGENT'
