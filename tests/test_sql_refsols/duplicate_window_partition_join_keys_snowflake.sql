SELECT
  o_custkey AS c1,
  o_orderkey AS oid,
  LAG(o_orderkey, 1) OVER (PARTITION BY o_custkey ORDER BY o_orderkey) AS prev_oid
FROM snowflake_sample_data.tpch_sf1.orders
WHERE
  o_custkey <= 2
ORDER BY
  1 NULLS FIRST,
  2 NULLS FIRST
