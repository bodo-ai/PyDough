WITH _table_alias_0 AS (
  SELECT
    orders.o_custkey AS customer_key,
    orders.o_orderkey AS key,
    orders.o_orderdate AS order_date,
    orders.o_totalprice AS total_price
  FROM tpch.orders AS orders
), _table_alias_1 AS (
  SELECT
    customer.c_custkey AS key,
    customer.c_name AS name
  FROM tpch.customer AS customer
), _table_alias_2 AS (
  SELECT
    _table_alias_0.key AS key,
    _table_alias_1.key AS key_2,
    _table_alias_1.name AS name,
    _table_alias_0.order_date AS order_date,
    _table_alias_0.total_price AS total_price
  FROM _table_alias_0 AS _table_alias_0
  LEFT JOIN _table_alias_1 AS _table_alias_1
    ON _table_alias_0.customer_key = _table_alias_1.key
), _table_alias_3 AS (
  SELECT
    SUM(lineitem.l_quantity) AS agg_0,
    lineitem.l_orderkey AS order_key
  FROM tpch.lineitem AS lineitem
  GROUP BY
    lineitem.l_orderkey
), _t0 AS (
  SELECT
    _table_alias_2.key_2 AS c_custkey,
    _table_alias_2.name AS c_name,
    _table_alias_2.order_date AS o_orderdate,
    _table_alias_2.key AS o_orderkey,
    _table_alias_2.total_price AS o_totalprice,
    COALESCE(_table_alias_3.agg_0, 0) AS total_quantity,
    _table_alias_2.total_price AS ordering_1,
    _table_alias_2.order_date AS ordering_2
  FROM _table_alias_2 AS _table_alias_2
  LEFT JOIN _table_alias_3 AS _table_alias_3
    ON _table_alias_2.key = _table_alias_3.order_key
  WHERE
    NOT _table_alias_3.agg_0 IS NULL AND _table_alias_3.agg_0 > 300
  ORDER BY
    ordering_1 DESC,
    ordering_2
  LIMIT 10
)
SELECT
  _t0.c_name AS C_NAME,
  _t0.c_custkey AS C_CUSTKEY,
  _t0.o_orderkey AS O_ORDERKEY,
  _t0.o_orderdate AS O_ORDERDATE,
  _t0.o_totalprice AS O_TOTALPRICE,
  _t0.total_quantity AS TOTAL_QUANTITY
FROM _t0 AS _t0
ORDER BY
  _t0.ordering_1 DESC,
  _t0.ordering_2
