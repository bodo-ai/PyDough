SELECT
  customer.c_custkey AS k,
  orders.o_orderkey AS oid,
  SUM(orders.o_totalprice) OVER (PARTITION BY customer.c_custkey) AS total
FROM tpch.customer AS customer
JOIN tpch.orders AS orders
  ON customer.c_custkey = orders.o_custkey AND orders.o_custkey <= 2
WHERE
  customer.c_custkey <= 2
