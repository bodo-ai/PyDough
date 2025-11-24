SELECT
  lineitem.l_shipmode AS L_SHIPMODE,
  COUNT_IF(orders.o_orderpriority IN ('1-URGENT', '2-HIGH')) AS HIGH_LINE_COUNT,
  COUNT_IF(NOT orders.o_orderpriority IN ('1-URGENT', '2-HIGH')) AS LOW_LINE_COUNT
FROM tpch.lineitem AS lineitem
JOIN tpch.orders AS orders
  ON lineitem.l_orderkey = orders.o_orderkey
WHERE
  YEAR(CAST(lineitem.l_receiptdate AS TIMESTAMP)) = 1994
  AND lineitem.l_commitdate < lineitem.l_receiptdate
  AND lineitem.l_commitdate > lineitem.l_shipdate
  AND (
    lineitem.l_shipmode = 'MAIL' OR lineitem.l_shipmode = 'SHIP'
  )
GROUP BY
  1
ORDER BY
  1 NULLS FIRST
