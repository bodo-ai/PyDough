SELECT
  orders.o_orderpriority AS O_ORDERPRIORITY,
  COUNT(*) AS ORDER_COUNT
FROM tpch.orders AS orders
JOIN tpch.lineitem AS lineitem
  ON lineitem.l_commitdate < lineitem.l_receiptdate
  AND lineitem.l_orderkey = orders.o_orderkey
WHERE
  EXTRACT(MONTH FROM CAST(orders.o_orderdate AS DATETIME)) IN (7, 8, 9)
  AND EXTRACT(YEAR FROM CAST(orders.o_orderdate AS DATETIME)) = 1993
GROUP BY
  orders.o_orderpriority
ORDER BY
  o_orderpriority
