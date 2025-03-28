SELECT
  orders.o_orderkey AS order_key,
  RANK() OVER (ORDER BY orders.o_orderpriority) AS rank
FROM tpch.orders AS orders
