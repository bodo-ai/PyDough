WITH _table_alias_2 AS (
  SELECT
    customer.c_acctbal AS acctbal,
    customer.c_address AS address,
    customer.c_comment AS comment,
    customer.c_custkey AS key,
    customer.c_name AS name,
    customer.c_nationkey AS nation_key,
    customer.c_phone AS phone
  FROM tpch.customer AS customer
), _table_alias_0 AS (
  SELECT
    orders.o_custkey AS customer_key,
    orders.o_orderkey AS key
  FROM tpch.orders AS orders
  WHERE
    orders.o_orderdate < '1994-01-01' AND orders.o_orderdate >= '1993-10-01'
), _table_alias_1 AS (
  SELECT
    lineitem.l_discount AS discount,
    lineitem.l_extendedprice AS extended_price,
    lineitem.l_orderkey AS order_key
  FROM tpch.lineitem AS lineitem
  WHERE
    lineitem.l_returnflag = 'R'
), _table_alias_3 AS (
  SELECT
    SUM(_table_alias_1.extended_price * (
      1 - _table_alias_1.discount
    )) AS agg_0,
    _table_alias_0.customer_key AS customer_key
  FROM _table_alias_0 AS _table_alias_0
  JOIN _table_alias_1 AS _table_alias_1
    ON _table_alias_0.key = _table_alias_1.order_key
  GROUP BY
    _table_alias_0.customer_key
), _table_alias_4 AS (
  SELECT
    _table_alias_2.acctbal AS acctbal,
    _table_alias_2.address AS address,
    _table_alias_3.agg_0 AS agg_0,
    _table_alias_2.comment AS comment,
    _table_alias_2.key AS key,
    _table_alias_2.name AS name,
    _table_alias_2.nation_key AS nation_key,
    _table_alias_2.phone AS phone
  FROM _table_alias_2 AS _table_alias_2
  LEFT JOIN _table_alias_3 AS _table_alias_3
    ON _table_alias_2.key = _table_alias_3.customer_key
), _table_alias_5 AS (
  SELECT
    nation.n_nationkey AS key,
    nation.n_name AS name
  FROM tpch.nation AS nation
), _t0 AS (
  SELECT
    _table_alias_4.acctbal AS c_acctbal,
    _table_alias_4.address AS c_address,
    _table_alias_4.comment AS c_comment,
    _table_alias_4.key AS c_custkey,
    _table_alias_4.name AS c_name,
    _table_alias_4.phone AS c_phone,
    _table_alias_5.name AS n_name,
    COALESCE(_table_alias_4.agg_0, 0) AS revenue,
    COALESCE(_table_alias_4.agg_0, 0) AS ordering_1,
    _table_alias_4.key AS ordering_2
  FROM _table_alias_4 AS _table_alias_4
  LEFT JOIN _table_alias_5 AS _table_alias_5
    ON _table_alias_4.nation_key = _table_alias_5.key
  ORDER BY
    ordering_1 DESC,
    ordering_2
  LIMIT 20
)
SELECT
  _t0.c_custkey AS C_CUSTKEY,
  _t0.c_name AS C_NAME,
  _t0.revenue AS REVENUE,
  _t0.c_acctbal AS C_ACCTBAL,
  _t0.n_name AS N_NAME,
  _t0.c_address AS C_ADDRESS,
  _t0.c_phone AS C_PHONE,
  _t0.c_comment AS C_COMMENT
FROM _t0 AS _t0
ORDER BY
  _t0.ordering_1 DESC,
  _t0.ordering_2
