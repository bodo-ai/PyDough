SELECT
  ANY_VALUE(customer.c_name) AS any
FROM tpch.customer AS customer
JOIN tpch.orders AS orders
  ON EXTRACT(YEAR FROM CAST(orders.o_orderdate AS DATETIME)) = 1994
  AND customer.c_custkey = orders.o_custkey
  AND orders.o_orderpriority = '1-URGENT'
