WITH _t1_2 AS (
  SELECT
    SUM(
      (
        orders.o_orderpriority <> '1-URGENT' AND orders.o_orderpriority <> '2-HIGH'
      )
    ) AS agg_1,
    SUM((
      orders.o_orderpriority = '1-URGENT' OR orders.o_orderpriority = '2-HIGH'
    )) AS agg_0,
    lineitem.l_shipmode AS ship_mode
  FROM tpch.lineitem AS lineitem
  LEFT JOIN tpch.orders AS orders
    ON lineitem.l_orderkey = orders.o_orderkey
  WHERE
    lineitem.l_commitdate < lineitem.l_receiptdate
    AND lineitem.l_commitdate > lineitem.l_shipdate
    AND lineitem.l_receiptdate < '1995-01-01'
    AND lineitem.l_receiptdate >= '1994-01-01'
    AND (
      lineitem.l_shipmode = 'MAIL' OR lineitem.l_shipmode = 'SHIP'
    )
  GROUP BY
    lineitem.l_shipmode
)
SELECT
  _t1.ship_mode AS L_SHIPMODE,
  COALESCE(_t1.agg_0, 0) AS HIGH_LINE_COUNT,
  COALESCE(_t1.agg_1, 0) AS LOW_LINE_COUNT
FROM _t1_2 AS _t1
ORDER BY
  _t1.ship_mode
