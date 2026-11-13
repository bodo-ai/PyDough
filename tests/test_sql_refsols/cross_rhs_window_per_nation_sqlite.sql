SELECT
  customer.c_custkey AS k,
  orders.o_orderkey AS oid,
  SUM(orders.o_totalprice) OVER (PARTITION BY customer_2.c_nationkey, customer.c_custkey) AS s
FROM tpch.customer AS customer
CROSS JOIN tpch.nation AS nation
JOIN tpch.customer AS customer_2
  ON customer_2.c_nationkey = nation.n_nationkey
JOIN tpch.orders AS orders
  ON customer_2.c_custkey = orders.o_custkey
WHERE
  customer.c_custkey <= 2
ORDER BY
  1,
  2
