SELECT
  l_shipmode COLLATE utf8mb4_bin AS L_SHIPMODE,
  COALESCE(SUM(orders.o_orderpriority IN ('1-URGENT', '2-HIGH')), 0) AS HIGH_LINE_COUNT,
  COALESCE(SUM(NOT orders.o_orderpriority IN ('1-URGENT', '2-HIGH')), 0) AS LOW_LINE_COUNT
FROM tpch.lineitem AS lineitem
JOIN tpch.orders AS orders
  ON lineitem.l_orderkey = orders.o_orderkey
WHERE
  EXTRACT(YEAR FROM CAST(lineitem.l_receiptdate AS DATETIME)) = 1994
  AND lineitem.l_commitdate < lineitem.l_receiptdate
  AND lineitem.l_commitdate > lineitem.l_shipdate
  AND (
    lineitem.l_shipmode = 'MAIL' OR lineitem.l_shipmode = 'SHIP'
  )
GROUP BY
  1
ORDER BY
  1
