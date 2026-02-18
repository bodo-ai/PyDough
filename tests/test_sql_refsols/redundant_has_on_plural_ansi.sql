SELECT
  COUNT(*) AS n
FROM tpch.customer AS customer
JOIN tpch.orders AS orders
  ON customer.c_custkey = orders.o_custkey AND orders.o_totalprice > 400000
