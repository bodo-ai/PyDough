WITH _t0 AS (
  SELECT
    COUNT(*) AS order_count,
    orders.o_orderpriority AS o_orderpriority
  FROM tpch.orders AS orders
  JOIN tpch.lineitem AS lineitem
    ON lineitem.l_commitdate < lineitem.l_receiptdate
    AND lineitem.l_orderkey = orders.o_orderkey
  WHERE
    EXTRACT(QUARTER FROM orders.o_orderdate) = 3
    AND EXTRACT(YEAR FROM orders.o_orderdate) = 1993
  GROUP BY
    orders.o_orderpriority
)
SELECT
  _t0.o_orderpriority AS O_ORDERPRIORITY,
  _t0.order_count AS ORDER_COUNT
FROM _t0 AS _t0
ORDER BY
  o_orderpriority
