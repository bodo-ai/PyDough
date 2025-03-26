SELECT
  orders.o_orderdate AS order_date,
  DENSE_RANK() OVER (ORDER BY orders.o_orderdate NULLS LAST) AS rank
FROM tpch.orders AS orders
