WITH _table_alias_2 AS (
  SELECT
    lineitem.l_discount AS discount,
    lineitem.l_extendedprice AS extended_price,
    lineitem.l_orderkey AS order_key,
    lineitem.l_shipdate AS ship_date,
    lineitem.l_suppkey AS supplier_key
  FROM tpch.lineitem AS lineitem
  WHERE
    lineitem.l_shipdate <= CAST('1996-12-31' AS DATE)
    AND lineitem.l_shipdate >= CAST('1995-01-01' AS DATE)
), _table_alias_0 AS (
  SELECT
    supplier.s_suppkey AS key,
    supplier.s_nationkey AS nation_key
  FROM tpch.supplier AS supplier
), _table_alias_1 AS (
  SELECT
    nation.n_nationkey AS key,
    nation.n_name AS name
  FROM tpch.nation AS nation
), _table_alias_3 AS (
  SELECT
    _table_alias_0.key AS key,
    _table_alias_1.name AS name_3
  FROM _table_alias_0 AS _table_alias_0
  JOIN _table_alias_1 AS _table_alias_1
    ON _table_alias_0.nation_key = _table_alias_1.key
), _table_alias_8 AS (
  SELECT
    _table_alias_2.discount AS discount,
    _table_alias_2.extended_price AS extended_price,
    _table_alias_3.name_3 AS name_3,
    _table_alias_2.order_key AS order_key,
    _table_alias_2.ship_date AS ship_date
  FROM _table_alias_2 AS _table_alias_2
  LEFT JOIN _table_alias_3 AS _table_alias_3
    ON _table_alias_2.supplier_key = _table_alias_3.key
  WHERE
    _table_alias_3.name_3 = 'FRANCE' OR _table_alias_3.name_3 = 'GERMANY'
), _table_alias_4 AS (
  SELECT
    orders.o_custkey AS customer_key,
    orders.o_orderkey AS key
  FROM tpch.orders AS orders
), _table_alias_5 AS (
  SELECT
    customer.c_custkey AS key,
    customer.c_nationkey AS nation_key
  FROM tpch.customer AS customer
), _table_alias_6 AS (
  SELECT
    _table_alias_4.key AS key,
    _table_alias_5.nation_key AS nation_key
  FROM _table_alias_4 AS _table_alias_4
  JOIN _table_alias_5 AS _table_alias_5
    ON _table_alias_4.customer_key = _table_alias_5.key
), _table_alias_9 AS (
  SELECT
    _table_alias_6.key AS key,
    _table_alias_7.name AS name_8
  FROM _table_alias_6 AS _table_alias_6
  JOIN _table_alias_1 AS _table_alias_7
    ON _table_alias_6.nation_key = _table_alias_7.key
), _t1 AS (
  SELECT
    SUM(_table_alias_8.extended_price * (
      1 - _table_alias_8.discount
    )) AS agg_0,
    _table_alias_9.name_8 AS cust_nation,
    EXTRACT(YEAR FROM CAST(_table_alias_8.ship_date AS DATETIME)) AS l_year,
    _table_alias_8.name_3 AS supp_nation
  FROM _table_alias_8 AS _table_alias_8
  LEFT JOIN _table_alias_9 AS _table_alias_9
    ON _table_alias_8.order_key = _table_alias_9.key
  WHERE
    (
      _table_alias_8.name_3 = 'FRANCE' OR _table_alias_9.name_8 = 'FRANCE'
    )
    AND (
      _table_alias_8.name_3 = 'GERMANY' OR _table_alias_9.name_8 = 'GERMANY'
    )
    AND (
      _table_alias_9.name_8 = 'FRANCE' OR _table_alias_9.name_8 = 'GERMANY'
    )
  GROUP BY
    _table_alias_9.name_8,
    EXTRACT(YEAR FROM CAST(_table_alias_8.ship_date AS DATETIME)),
    _table_alias_8.name_3
)
SELECT
  _t1.supp_nation AS SUPP_NATION,
  _t1.cust_nation AS CUST_NATION,
  _t1.l_year AS L_YEAR,
  COALESCE(_t1.agg_0, 0) AS REVENUE
FROM _t1 AS _t1
ORDER BY
  _t1.supp_nation,
  _t1.cust_nation,
  _t1.l_year
