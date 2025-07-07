SELECT
  lineitem.l_shipmode AS L_SHIPMODE,
  COALESCE(SUM(orders.o_orderpriority IN ('1-URGENT', '2-HIGH')), 0) AS HIGH_LINE_COUNT,
  COALESCE(SUM(NOT orders.o_orderpriority IN ('1-URGENT', '2-HIGH')), 0) AS LOW_LINE_COUNT
FROM tpch.lineitem AS lineitem
JOIN tpch.orders AS orders
  ON lineitem.l_orderkey = orders.o_orderkey
WHERE
  CAST(STRFTIME('%Y', lineitem.l_receiptdate) AS INTEGER) = 1994
  AND lineitem.l_commitdate < lineitem.l_receiptdate
  AND lineitem.l_commitdate > lineitem.l_shipdate
  AND (
    lineitem.l_shipmode = 'MAIL' OR lineitem.l_shipmode = 'SHIP'
  )
GROUP BY
  lineitem.l_shipmode
ORDER BY
  l_shipmode
