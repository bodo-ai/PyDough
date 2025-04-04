SELECT
  o_orderkey AS order_key,
  RANK() OVER (ORDER BY o_orderpriority) AS rank
FROM tpch.orders
