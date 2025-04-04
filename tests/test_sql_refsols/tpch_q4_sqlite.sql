WITH _t3 AS (
  SELECT
    o_orderdate AS order_date,
    o_orderkey AS key,
    o_orderpriority AS order_priority
  FROM tpch.orders
  WHERE
    o_orderdate < '1993-10-01' AND o_orderdate >= '1993-07-01'
), _t0 AS (
  SELECT
    key AS key,
    order_priority AS order_priority
  FROM _t3
), _t4 AS (
  SELECT
    l_commitdate AS commit_date,
    l_orderkey AS order_key,
    l_receiptdate AS receipt_date
  FROM tpch.lineitem
  WHERE
    l_commitdate < l_receiptdate
), _t1 AS (
  SELECT
    order_key AS order_key
  FROM _t4
), _t2 AS (
  SELECT
    order_priority AS order_priority
  FROM _t0
  WHERE
    EXISTS(
      SELECT
        1 AS "1"
      FROM _t1
      WHERE
        key = order_key
    )
), _t1_2 AS (
  SELECT
    COUNT() AS agg_0,
    order_priority AS order_priority
  FROM _t2
  GROUP BY
    order_priority
), _t0_2 AS (
  SELECT
    COALESCE(_t1.agg_0, 0) AS order_count,
    _t1.order_priority AS o_orderpriority,
    _t1.order_priority AS ordering_1
  FROM _t1_2 AS _t1
)
SELECT
  _t0.o_orderpriority AS O_ORDERPRIORITY,
  _t0.order_count AS ORDER_COUNT
FROM _t0_2 AS _t0
ORDER BY
  _t0.ordering_1
