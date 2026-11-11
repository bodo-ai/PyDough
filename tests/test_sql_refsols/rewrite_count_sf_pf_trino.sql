SELECT
  COUNT(*) AS n
FROM mysql.tpch.orders AS orders
JOIN postgres.tpch.customer AS customer
  ON customer.c_custkey = orders.o_custkey AND customer.c_mktsegment = 'AUTOMOBILE'
WHERE
  orders.o_orderstatus = 'F'
