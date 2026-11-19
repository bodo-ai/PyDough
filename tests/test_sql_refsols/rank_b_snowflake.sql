SELECT
  o_orderkey AS order_key,
  RANK() OVER (ORDER BY o_orderpriority) AS rank
FROM snowflake_sample_data.tpch_sf1.orders
