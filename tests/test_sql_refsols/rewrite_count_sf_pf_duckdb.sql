SELECT
  COUNT(*) AS n
FROM tpch.orders AS orders
JOIN tpch.customer AS customer
  ON customer.c_custkey = orders.o_custkey AND customer.c_mktsegment = 'AUTOMOBILE'
WHERE
  orders.o_orderstatus = 'F'
