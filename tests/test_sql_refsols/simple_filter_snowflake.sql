SELECT
  o_orderdate AS order_date,
  o_orderkey,
  o_totalprice
FROM snowflake_sample_data.tpch_sf1.orders
WHERE
  o_totalprice < 1000.0
