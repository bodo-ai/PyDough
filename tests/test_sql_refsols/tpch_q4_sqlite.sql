WITH _t3 AS (
  SELECT
    orders.o_orderdate AS order_date,
    orders.o_orderkey AS key,
    orders.o_orderpriority AS order_priority
  FROM tpch.orders AS orders
  WHERE
    orders.o_orderdate < '1993-10-01' AND orders.o_orderdate >= '1993-07-01'
), _table_alias_0 AS (
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
), _table_alias_1 AS (
  SELECT
    _t4.order_key AS order_key
  FROM _t4 AS _t4
), _u_0 AS (
  SELECT
    _table_alias_1.order_key AS _u_1
  FROM _table_alias_1 AS _table_alias_1
  GROUP BY
    _table_alias_1.order_key
), _t2 AS (
  SELECT
    _table_alias_0.order_priority AS order_priority
  FROM _table_alias_0 AS _table_alias_0
  LEFT JOIN _u_0 AS _u_0
    ON _table_alias_0.key = _u_0._u_1
  WHERE
    NOT _u_0._u_1 IS NULL
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
    _t1.order_priority AS o_orderpriority,
    _t1.order_priority AS ordering_1
  FROM _t1 AS _t1
)
SELECT
  _t0.o_orderpriority AS O_ORDERPRIORITY,
  _t0.order_count AS ORDER_COUNT
FROM _t0 AS _t0
ORDER BY
  _t0.ordering_1
