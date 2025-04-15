WITH _t3 AS (
  SELECT
    orders.o_orderdate AS order_date,
    orders.o_orderkey AS key,
    orders.o_orderpriority AS order_priority
  FROM tpch.orders AS orders
  WHERE
    orders.o_orderdate < CAST('1993-10-01' AS DATE)
    AND orders.o_orderdate >= CAST('1993-07-01' AS DATE)
), _s0 AS (
  SELECT
    _t3.key AS key,
    _t3.order_priority AS order_priority
  FROM _t3 AS _t3
), _t4 AS (
  SELECT
    lineitem.l_commitdate AS commit_date,
    lineitem.l_orderkey AS order_key,
    lineitem.l_receiptdate AS receipt_date
  FROM tpch.lineitem AS lineitem
  WHERE
    lineitem.l_commitdate < lineitem.l_receiptdate
), _s1 AS (
  SELECT
    _t4.order_key AS order_key
  FROM _t4 AS _t4
), _t2 AS (
  SELECT
    _s0.order_priority AS order_priority
  FROM _s0 AS _s0
  WHERE
    EXISTS(
      SELECT
        1 AS "1"
      FROM _s1 AS _s1
      WHERE
        _s0.key = _s1.order_key
    )
), _t1 AS (
  SELECT
    COUNT() AS agg_0,
    _t2.order_priority AS order_priority
  FROM _t2 AS _t2
  GROUP BY
    _t2.order_priority
), _t0 AS (
  SELECT
    COALESCE(_t1.agg_0, 0) AS order_count,
    _t1.order_priority AS o_orderpriority
  FROM _t1 AS _t1
)
SELECT
  _t0.o_orderpriority AS O_ORDERPRIORITY,
  _t0.order_count AS ORDER_COUNT
FROM _t0 AS _t0
ORDER BY
  o_orderpriority
