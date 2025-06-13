WITH _t2 AS (
  SELECT
    orders.o_orderkey AS key,
    orders.o_orderdate AS order_date,
    orders.o_orderpriority AS order_priority
  FROM tpch.orders AS orders
), _s0 AS (
  SELECT
    _t2.key AS key,
    _t2.order_priority AS order_priority
  FROM _t2 AS _t2
  WHERE
    CASE
      WHEN CAST(STRFTIME('%m', _t2.order_date) AS INTEGER) <= 3
      AND CAST(STRFTIME('%m', _t2.order_date) AS INTEGER) >= 1
      THEN 1
      WHEN CAST(STRFTIME('%m', _t2.order_date) AS INTEGER) <= 6
      AND CAST(STRFTIME('%m', _t2.order_date) AS INTEGER) >= 4
      THEN 2
      WHEN CAST(STRFTIME('%m', _t2.order_date) AS INTEGER) <= 9
      AND CAST(STRFTIME('%m', _t2.order_date) AS INTEGER) >= 7
      THEN 3
      WHEN CAST(STRFTIME('%m', _t2.order_date) AS INTEGER) <= 12
      AND CAST(STRFTIME('%m', _t2.order_date) AS INTEGER) >= 10
      THEN 4
    END = 3
    AND CAST(STRFTIME('%Y', _t2.order_date) AS INTEGER) = 1993
), _t3 AS (
  SELECT
    lineitem.l_commitdate AS commit_date,
    lineitem.l_orderkey AS order_key,
    lineitem.l_receiptdate AS receipt_date
  FROM tpch.lineitem AS lineitem
), _s1 AS (
  SELECT
    _t3.order_key AS order_key
  FROM _t3 AS _t3
  WHERE
    _t3.commit_date < _t3.receipt_date
), _t1 AS (
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
), _t0 AS (
  SELECT
    COUNT() AS order_count,
    _t1.order_priority AS o_orderpriority
  FROM _t1 AS _t1
  GROUP BY
    _t1.order_priority
)
SELECT
  _t0.o_orderpriority AS O_ORDERPRIORITY,
  _t0.order_count AS ORDER_COUNT
FROM _t0 AS _t0
ORDER BY
  o_orderpriority
