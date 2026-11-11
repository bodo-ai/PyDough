SELECT
  COUNT(*) AS n
FROM snowflake_sample_data.tpch_sf1.partsupp AS partsupp
JOIN snowflake_sample_data.tpch_sf1.part AS part
  ON part.p_brand = 'Brand#23' AND part.p_partkey = partsupp.ps_partkey
