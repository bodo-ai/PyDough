SELECT
  region.r_regionkey AS rk,
  customer.c_custkey AS k,
  orders.o_orderkey AS oid,
  SUM(orders.o_totalprice) OVER (PARTITION BY customer.c_custkey, region.r_regionkey) AS t
FROM tpch.region AS region
JOIN tpch.customer AS customer
  ON customer.c_custkey <= 2
JOIN tpch.orders AS orders
  ON orders.o_custkey <= 2
WHERE
  region.r_regionkey <= 1
