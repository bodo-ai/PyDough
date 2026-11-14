SELECT
  customer.c_custkey AS k,
  orders.o_orderkey AS oid,
  ROW_NUMBER() OVER (PARTITION BY customer.c_custkey ORDER BY orders.o_orderkey) AS r,
  SUM(orders.o_totalprice) OVER (PARTITION BY customer.c_custkey) AS tot
FROM tpch.customer AS customer
JOIN tpch.orders AS orders
  ON orders.o_custkey <= 2
WHERE
  customer.c_custkey <= 2
