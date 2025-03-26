WITH _table_alias_0 AS (
  SELECT
    nation.n_name AS name,
    nation.n_name AS nation_name,
    nation.n_nationkey AS key,
    nation.n_regionkey AS region_key
  FROM tpch.nation AS nation
), _table_alias_1 AS (
  SELECT
    region.r_regionkey AS key
  FROM tpch.region AS region
  WHERE
    region.r_name = 'ASIA'
), _table_alias_2 AS (
  SELECT
    _table_alias_0.key AS key,
    _table_alias_0.name AS name,
    _table_alias_0.nation_name AS nation_name
  FROM _table_alias_0 AS _table_alias_0
  JOIN _table_alias_1 AS _table_alias_1
    ON _table_alias_0.region_key = _table_alias_1.key
), _table_alias_3 AS (
  SELECT
    customer.c_custkey AS key,
    customer.c_nationkey AS nation_key
  FROM tpch.customer AS customer
), _table_alias_4 AS (
  SELECT
    _table_alias_2.key AS key,
    _table_alias_3.key AS key_5,
    _table_alias_2.name AS name,
    _table_alias_2.nation_name AS nation_name
  FROM _table_alias_2 AS _table_alias_2
  JOIN _table_alias_3 AS _table_alias_3
    ON _table_alias_2.key = _table_alias_3.nation_key
), _table_alias_5 AS (
  SELECT
    orders.o_custkey AS customer_key,
    orders.o_orderkey AS key
  FROM tpch.orders AS orders
  WHERE
    orders.o_orderdate < CAST('1995-01-01' AS DATE)
    AND orders.o_orderdate >= CAST('1994-01-01' AS DATE)
), _table_alias_6 AS (
  SELECT
    _table_alias_4.key AS key,
    _table_alias_5.key AS key_8,
    _table_alias_4.name AS name,
    _table_alias_4.nation_name AS nation_name
  FROM _table_alias_4 AS _table_alias_4
  JOIN _table_alias_5 AS _table_alias_5
    ON _table_alias_4.key_5 = _table_alias_5.customer_key
), _table_alias_7 AS (
  SELECT
    lineitem.l_discount AS discount,
    lineitem.l_extendedprice AS extended_price,
    lineitem.l_orderkey AS order_key,
    lineitem.l_suppkey AS supplier_key
  FROM tpch.lineitem AS lineitem
), _table_alias_10 AS (
  SELECT
    _table_alias_7.discount AS discount,
    _table_alias_7.extended_price AS extended_price,
    _table_alias_6.key AS key,
    _table_alias_6.name AS name,
    _table_alias_6.nation_name AS nation_name,
    _table_alias_7.supplier_key AS supplier_key
  FROM _table_alias_6 AS _table_alias_6
  JOIN _table_alias_7 AS _table_alias_7
    ON _table_alias_6.key_8 = _table_alias_7.order_key
), _table_alias_8 AS (
  SELECT
    supplier.s_suppkey AS key,
    supplier.s_nationkey AS nation_key
  FROM tpch.supplier AS supplier
), _table_alias_9 AS (
  SELECT
    nation.n_nationkey AS key,
    nation.n_name AS name
  FROM tpch.nation AS nation
), _table_alias_11 AS (
  SELECT
    _table_alias_8.key AS key,
    _table_alias_9.name AS name_12
  FROM _table_alias_8 AS _table_alias_8
  JOIN _table_alias_9 AS _table_alias_9
    ON _table_alias_8.nation_key = _table_alias_9.key
), _t1 AS (
  SELECT
    ANY_VALUE(_table_alias_10.name) AS agg_3,
    SUM(_table_alias_10.extended_price * (
      1 - _table_alias_10.discount
    )) AS agg_0,
    _table_alias_10.key AS key
  FROM _table_alias_10 AS _table_alias_10
  LEFT JOIN _table_alias_11 AS _table_alias_11
    ON _table_alias_10.supplier_key = _table_alias_11.key
  WHERE
    _table_alias_10.nation_name = _table_alias_11.name_12
  GROUP BY
    _table_alias_10.key
)
SELECT
  _t1.agg_3 AS N_NAME,
  COALESCE(_t1.agg_0, 0) AS REVENUE
FROM _t1 AS _t1
ORDER BY
  COALESCE(_t1.agg_0, 0) DESC
