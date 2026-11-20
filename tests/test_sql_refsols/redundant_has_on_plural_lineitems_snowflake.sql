SELECT
  COUNT(DISTINCT l_orderkey) AS n
FROM snowflake_sample_data.tpch_sf1.lineitem
WHERE
  l_quantity > 49
