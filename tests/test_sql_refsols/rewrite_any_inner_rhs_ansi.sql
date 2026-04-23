SELECT
  ANY_VALUE(customer.c_name) AS any_customer
FROM tpch.customer AS customer
JOIN tpch.orders AS orders
  ON customer.c_custkey = orders.o_custkey
  AND orders.o_clerk = 'Clerk#000000470'
  AND orders.o_totalprice = 252004.18
