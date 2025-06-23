WITH _t0 AS (
  SELECT
    COALESCE(SUM(orders.o_orderpriority IN ('1-URGENT', '2-HIGH')), 0) AS high_line_count,
    COALESCE(SUM(NOT orders.o_orderpriority IN ('1-URGENT', '2-HIGH')), 0) AS low_line_count,
    lineitem.l_shipmode
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
)
SELECT
  l_shipmode AS L_SHIPMODE,
  high_line_count AS HIGH_LINE_COUNT,
  low_line_count AS LOW_LINE_COUNT
FROM _t0
ORDER BY
  l_shipmode
