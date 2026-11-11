SELECT
  COUNT(*) AS n
FROM tpch_s3_pq.tpch_sf1.orders AS orders
JOIN tpch_s3_pq.tpch_sf1.customer AS customer
  ON customer.c_custkey = orders.o_custkey AND customer.c_mktsegment = 'AUTOMOBILE'
WHERE
  orders.o_orderstatus = 'F'
