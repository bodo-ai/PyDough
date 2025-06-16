WITH _t2 AS (
  SELECT
    orders.o_orderdate AS o_orderdate,
    orders.o_orderkey AS o_orderkey,
    orders.o_orderpriority AS o_orderpriority
  FROM tpch.orders AS orders
), _s0 AS (
  SELECT
    _t2.o_orderkey AS o_orderkey,
    _t2.o_orderpriority AS o_orderpriority
  FROM _t2 AS _t2
  WHERE
    EXTRACT(QUARTER FROM _t2.o_orderdate) = 3
    AND EXTRACT(YEAR FROM _t2.o_orderdate) = 1993
), _t3 AS (
  SELECT
    lineitem.l_commitdate AS l_commitdate,
    lineitem.l_orderkey AS l_orderkey,
    lineitem.l_receiptdate AS l_receiptdate
  FROM tpch.lineitem AS lineitem
), _s1 AS (
  SELECT
    _t3.l_orderkey AS l_orderkey
  FROM _t3 AS _t3
  WHERE
    _t3.l_commitdate < _t3.l_receiptdate
), _t1 AS (
  SELECT
    _s0.o_orderpriority AS o_orderpriority
  FROM _s0 AS _s0
  WHERE
    EXISTS(
      SELECT
        1 AS "1"
      FROM _s1 AS _s1
      WHERE
        _s0.o_orderkey = _s1.l_orderkey
    )
), _t0 AS (
  SELECT
    COUNT() AS order_count,
    _t1.o_orderpriority AS order_priority
  FROM _t1 AS _t1
  GROUP BY
    _t1.o_orderpriority
)
SELECT
  _t0.order_priority AS O_ORDERPRIORITY,
  _t0.order_count AS ORDER_COUNT
FROM _t0 AS _t0
ORDER BY
  _t0.order_priority
