SELECT
  o_orderkey AS oid,
  o_totalprice AS amount
FROM snowflake_sample_data.tpch_sf1.orders
ORDER BY
  2 DESC NULLS LAST,
  1 NULLS FIRST
LIMIT 5
