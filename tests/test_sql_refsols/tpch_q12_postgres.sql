SELECT
  lineitem.l_shipmode AS L_SHIPMODE,
  COALESCE(
    SUM(CASE WHEN orders.o_orderpriority IN ('1-URGENT', '2-HIGH') THEN 1 ELSE 0 END),
    0
  ) AS HIGH_LINE_COUNT,
  COALESCE(
    SUM(CASE WHEN NOT orders.o_orderpriority IN ('1-URGENT', '2-HIGH') THEN 1 ELSE 0 END),
    0
  ) AS LOW_LINE_COUNT
FROM tpch.lineitem AS lineitem
JOIN tpch.orders AS orders
  ON lineitem.l_orderkey = orders.o_orderkey
WHERE
  EXTRACT(YEAR FROM CAST(lineitem.l_receiptdate AS TIMESTAMP)) = 1994
  AND lineitem.l_commitdate < lineitem.l_receiptdate
  AND lineitem.l_commitdate > lineitem.l_shipdate
  AND (
    lineitem.l_shipmode = 'MAIL' OR lineitem.l_shipmode = 'SHIP'
  )
GROUP BY
  lineitem.l_shipmode
ORDER BY
  l_shipmode NULLS FIRST
