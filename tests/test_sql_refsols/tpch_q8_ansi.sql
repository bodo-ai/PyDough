WITH _table_alias_0 AS (
  SELECT
    nation.n_name AS nation_name,
    nation.n_nationkey AS key
  FROM tpch.nation AS nation
), _table_alias_1 AS (
  SELECT
    supplier.s_suppkey AS key,
    supplier.s_nationkey AS nation_key
  FROM tpch.supplier AS supplier
), _table_alias_2 AS (
  SELECT
    _table_alias_1.key AS key_2,
    _table_alias_0.nation_name AS nation_name
  FROM _table_alias_0 AS _table_alias_0
  JOIN _table_alias_1 AS _table_alias_1
    ON _table_alias_0.key = _table_alias_1.nation_key
), _table_alias_3 AS (
  SELECT
    partsupp.ps_partkey AS part_key,
    partsupp.ps_suppkey AS supplier_key
  FROM tpch.partsupp AS partsupp
), _table_alias_4 AS (
  SELECT
    _table_alias_2.nation_name AS nation_name,
    _table_alias_3.part_key AS part_key,
    _table_alias_3.supplier_key AS supplier_key
  FROM _table_alias_2 AS _table_alias_2
  JOIN _table_alias_3 AS _table_alias_3
    ON _table_alias_2.key_2 = _table_alias_3.supplier_key
), _table_alias_5 AS (
  SELECT
    part.p_partkey AS key
  FROM tpch.part AS part
  WHERE
    part.p_type = 'ECONOMY ANODIZED STEEL'
), _table_alias_6 AS (
  SELECT
    _table_alias_4.nation_name AS nation_name,
    _table_alias_4.part_key AS part_key,
    _table_alias_4.supplier_key AS supplier_key
  FROM _table_alias_4 AS _table_alias_4
  JOIN _table_alias_5 AS _table_alias_5
    ON _table_alias_4.part_key = _table_alias_5.key
), _table_alias_7 AS (
  SELECT
    lineitem.l_discount AS discount,
    lineitem.l_extendedprice AS extended_price,
    lineitem.l_orderkey AS order_key,
    lineitem.l_partkey AS part_key,
    lineitem.l_suppkey AS supplier_key
  FROM tpch.lineitem AS lineitem
), _table_alias_8 AS (
  SELECT
    _table_alias_7.extended_price * (
      1 - _table_alias_7.discount
    ) AS volume,
    _table_alias_6.nation_name AS nation_name,
    _table_alias_7.order_key AS order_key
  FROM _table_alias_6 AS _table_alias_6
  JOIN _table_alias_7 AS _table_alias_7
    ON _table_alias_6.part_key = _table_alias_7.part_key
    AND _table_alias_6.supplier_key = _table_alias_7.supplier_key
), _table_alias_9 AS (
  SELECT
    orders.o_custkey AS customer_key,
    orders.o_orderkey AS key,
    orders.o_orderdate AS order_date
  FROM tpch.orders AS orders
  WHERE
    orders.o_orderdate <= CAST('1996-12-31' AS DATE)
    AND orders.o_orderdate >= CAST('1995-01-01' AS DATE)
), _table_alias_14 AS (
  SELECT
    CASE
      WHEN _table_alias_8.nation_name = 'BRAZIL'
      THEN _table_alias_8.volume
      ELSE 0
    END AS brazil_volume,
    EXTRACT(YEAR FROM CAST(_table_alias_9.order_date AS DATETIME)) AS o_year,
    _table_alias_9.customer_key AS customer_key,
    _table_alias_8.volume AS volume
  FROM _table_alias_8 AS _table_alias_8
  JOIN _table_alias_9 AS _table_alias_9
    ON _table_alias_8.order_key = _table_alias_9.key
), _table_alias_10 AS (
  SELECT
    customer.c_custkey AS key,
    customer.c_nationkey AS nation_key
  FROM tpch.customer AS customer
), _table_alias_11 AS (
  SELECT
    nation.n_nationkey AS key,
    nation.n_regionkey AS region_key
  FROM tpch.nation AS nation
), _table_alias_12 AS (
  SELECT
    _table_alias_10.key AS key,
    _table_alias_11.region_key AS region_key
  FROM _table_alias_10 AS _table_alias_10
  JOIN _table_alias_11 AS _table_alias_11
    ON _table_alias_10.nation_key = _table_alias_11.key
), _table_alias_13 AS (
  SELECT
    region.r_regionkey AS key
  FROM tpch.region AS region
  WHERE
    region.r_name = 'AMERICA'
), _table_alias_15 AS (
  SELECT
    _table_alias_12.key AS key
  FROM _table_alias_12 AS _table_alias_12
  JOIN _table_alias_13 AS _table_alias_13
    ON _table_alias_12.region_key = _table_alias_13.key
), _t0 AS (
  SELECT
    SUM(_table_alias_14.brazil_volume) AS agg_0,
    SUM(_table_alias_14.volume) AS agg_1,
    _table_alias_14.o_year AS o_year
  FROM _table_alias_14 AS _table_alias_14
  JOIN _table_alias_15 AS _table_alias_15
    ON _table_alias_14.customer_key = _table_alias_15.key
  GROUP BY
    _table_alias_14.o_year
)
SELECT
  _t0.o_year AS O_YEAR,
  COALESCE(_t0.agg_0, 0) / COALESCE(_t0.agg_1, 0) AS MKT_SHARE
FROM _t0 AS _t0
