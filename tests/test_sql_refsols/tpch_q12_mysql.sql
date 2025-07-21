SELECT
  LINEITEM.l_shipmode AS L_SHIPMODE,
  COALESCE(SUM(ORDERS.o_orderpriority IN ('1-URGENT', '2-HIGH')), 0) AS HIGH_LINE_COUNT,
  COALESCE(SUM(NOT ORDERS.o_orderpriority IN ('1-URGENT', '2-HIGH')), 0) AS LOW_LINE_COUNT
FROM tpch.LINEITEM AS LINEITEM
JOIN tpch.ORDERS AS ORDERS
  ON LINEITEM.l_orderkey = ORDERS.o_orderkey
WHERE
  LINEITEM.l_commitdate < LINEITEM.l_receiptdate
  AND LINEITEM.l_commitdate > LINEITEM.l_shipdate
  AND (
    LINEITEM.l_shipmode = 'MAIL' OR LINEITEM.l_shipmode = 'SHIP'
  )
  AND YEAR(LINEITEM.l_receiptdate) = 1994
GROUP BY
  LINEITEM.l_shipmode
ORDER BY
  LINEITEM.l_shipmode
