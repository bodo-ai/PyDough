SELECT
  COUNT(*) AS n
FROM snowflake_sample_data.tpch_sf1.orders AS orders
JOIN snowflake_sample_data.tpch_sf1.customer AS customer
  ON customer.c_custkey = orders.o_custkey AND customer.c_mktsegment = 'AUTOMOBILE'
WHERE
  orders.o_orderstatus = 'F'
