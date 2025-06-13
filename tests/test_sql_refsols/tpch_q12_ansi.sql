WITH _t1 AS (
  SELECT
    SUM(orders.o_orderpriority IN ('1-URGENT', '2-HIGH')) AS agg_0,
    SUM(NOT orders.o_orderpriority IN ('1-URGENT', '2-HIGH')) AS agg_1,
    lineitem.l_shipmode AS ship_mode
  FROM tpch.lineitem AS lineitem
  JOIN tpch.orders AS orders
    ON lineitem.l_orderkey = orders.o_orderkey
  WHERE
    EXTRACT(YEAR FROM lineitem.l_receiptdate) = 1994
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
  COALESCE(agg_0, 0) AS HIGH_LINE_COUNT,
  COALESCE(agg_1, 0) AS LOW_LINE_COUNT
FROM _t1
ORDER BY
  l_shipmode
