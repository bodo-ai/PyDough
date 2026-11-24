SELECT
  nation.n_nationkey AS nk,
  orders.o_orderkey AS oid,
  SUM(orders.o_totalprice) OVER (PARTITION BY orders.o_custkey, nation.n_nationkey) AS s
FROM tpch.nation AS nation
CROSS JOIN tpch.customer AS customer
JOIN tpch.orders AS orders
  ON customer.c_custkey = orders.o_custkey AND orders.o_custkey <= 2
WHERE
  nation.n_nationkey <= 1
