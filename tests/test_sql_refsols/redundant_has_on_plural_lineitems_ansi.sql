SELECT
  COUNT(*) AS n
FROM tpch.orders AS orders
JOIN tpch.lineitem AS lineitem
  ON lineitem.l_orderkey = orders.o_orderkey AND lineitem.l_quantity > 49
