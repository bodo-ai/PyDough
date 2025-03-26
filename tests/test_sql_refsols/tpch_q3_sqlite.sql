WITH _table_alias_0 AS (
  SELECT
    orders.o_custkey AS customer_key,
    orders.o_orderkey AS key,
    orders.o_orderdate AS order_date,
    orders.o_shippriority AS ship_priority
  FROM tpch.orders AS orders
  WHERE
    orders.o_orderdate < '1995-03-15'
), _table_alias_1 AS (
  SELECT
    customer.c_custkey AS key
  FROM tpch.customer AS customer
  WHERE
    customer.c_mktsegment = 'BUILDING'
), _table_alias_2 AS (
  SELECT
    _table_alias_0.key AS key,
    _table_alias_0.order_date AS order_date,
    _table_alias_0.ship_priority AS ship_priority
  FROM _table_alias_0 AS _table_alias_0
  JOIN _table_alias_1 AS _table_alias_1
    ON _table_alias_0.customer_key = _table_alias_1.key
), _table_alias_3 AS (
  SELECT
    lineitem.l_discount AS discount,
    lineitem.l_extendedprice AS extended_price,
    lineitem.l_orderkey AS order_key
  FROM tpch.lineitem AS lineitem
  WHERE
    lineitem.l_shipdate > '1995-03-15'
), _t2 AS (
  SELECT
    SUM(_table_alias_3.extended_price * (
      1 - _table_alias_3.discount
    )) AS agg_0,
    _table_alias_2.order_date AS order_date,
    _table_alias_3.order_key AS order_key,
    _table_alias_2.ship_priority AS ship_priority
  FROM _table_alias_2 AS _table_alias_2
  JOIN _table_alias_3 AS _table_alias_3
    ON _table_alias_2.key = _table_alias_3.order_key
  GROUP BY
    _table_alias_2.order_date,
    _table_alias_3.order_key,
    _table_alias_2.ship_priority
), _t0 AS (
  SELECT
    _t2.order_key AS l_orderkey,
    _t2.order_date AS o_orderdate,
    _t2.ship_priority AS o_shippriority,
    COALESCE(_t2.agg_0, 0) AS revenue,
    COALESCE(_t2.agg_0, 0) AS ordering_1,
    _t2.order_date AS ordering_2,
    _t2.order_key AS ordering_3
  FROM _t2 AS _t2
  ORDER BY
    ordering_1 DESC,
    ordering_2,
    ordering_3
  LIMIT 10
)
SELECT
  _t0.l_orderkey AS L_ORDERKEY,
  _t0.revenue AS REVENUE,
  _t0.o_orderdate AS O_ORDERDATE,
  _t0.o_shippriority AS O_SHIPPRIORITY
FROM _t0 AS _t0
ORDER BY
  _t0.ordering_1 DESC,
  _t0.ordering_2,
  _t0.ordering_3
