WITH _t1 AS (
  SELECT
    lineitem.l_shipmode AS ship_mode,
    SUM(NOT orders.o_orderpriority IN ('1-URGENT', '2-HIGH')) AS sum_expr_2,
    SUM(orders.o_orderpriority IN ('1-URGENT', '2-HIGH')) AS sum_is_high_priority
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
  ship_mode AS L_SHIPMODE,
  COALESCE(sum_is_high_priority, 0) AS HIGH_LINE_COUNT,
  COALESCE(sum_expr_2, 0) AS LOW_LINE_COUNT
FROM _t1
ORDER BY
  ship_mode
