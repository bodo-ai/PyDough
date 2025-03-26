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
    partsupp.ps_suppkey AS supplier_key,
    partsupp.ps_supplycost AS supplycost
  FROM tpch.partsupp AS partsupp
), _table_alias_4 AS (
  SELECT
    _table_alias_2.nation_name AS nation_name,
    _table_alias_3.part_key AS part_key,
    _table_alias_3.supplier_key AS supplier_key,
    _table_alias_3.supplycost AS supplycost
  FROM _table_alias_2 AS _table_alias_2
  JOIN _table_alias_3 AS _table_alias_3
    ON _table_alias_2.key_2 = _table_alias_3.supplier_key
), _table_alias_5 AS (
  SELECT
    part.p_partkey AS key
  FROM tpch.part AS part
  WHERE
    part.p_name LIKE '%green%'
), _table_alias_6 AS (
  SELECT
    _table_alias_4.nation_name AS nation_name,
    _table_alias_4.part_key AS part_key,
    _table_alias_4.supplier_key AS supplier_key,
    _table_alias_4.supplycost AS supplycost
  FROM _table_alias_4 AS _table_alias_4
  JOIN _table_alias_5 AS _table_alias_5
    ON _table_alias_4.part_key = _table_alias_5.key
), _table_alias_7 AS (
  SELECT
    lineitem.l_discount AS discount,
    lineitem.l_extendedprice AS extended_price,
    lineitem.l_orderkey AS order_key,
    lineitem.l_partkey AS part_key,
    lineitem.l_quantity AS quantity,
    lineitem.l_suppkey AS supplier_key
  FROM tpch.lineitem AS lineitem
), _table_alias_8 AS (
  SELECT
    _table_alias_7.discount AS discount,
    _table_alias_7.extended_price AS extended_price,
    _table_alias_6.nation_name AS nation_name,
    _table_alias_7.order_key AS order_key,
    _table_alias_7.quantity AS quantity,
    _table_alias_6.supplycost AS supplycost
  FROM _table_alias_6 AS _table_alias_6
  JOIN _table_alias_7 AS _table_alias_7
    ON _table_alias_6.part_key = _table_alias_7.part_key
    AND _table_alias_6.supplier_key = _table_alias_7.supplier_key
), _table_alias_9 AS (
  SELECT
    orders.o_orderkey AS key,
    orders.o_orderdate AS order_date
  FROM tpch.orders AS orders
), _t2 AS (
  SELECT
    SUM(
      _table_alias_8.extended_price * (
        1 - _table_alias_8.discount
      ) - _table_alias_8.supplycost * _table_alias_8.quantity
    ) AS agg_0,
    _table_alias_8.nation_name AS nation_name,
    EXTRACT(YEAR FROM CAST(_table_alias_9.order_date AS DATETIME)) AS o_year
  FROM _table_alias_8 AS _table_alias_8
  LEFT JOIN _table_alias_9 AS _table_alias_9
    ON _table_alias_8.order_key = _table_alias_9.key
  GROUP BY
    _table_alias_8.nation_name,
    EXTRACT(YEAR FROM CAST(_table_alias_9.order_date AS DATETIME))
), _t0 AS (
  SELECT
    COALESCE(_t2.agg_0, 0) AS amount,
    _t2.nation_name AS nation,
    _t2.o_year AS o_year,
    _t2.nation_name AS ordering_1,
    _t2.o_year AS ordering_2
  FROM _t2 AS _t2
  ORDER BY
    ordering_1,
    ordering_2 DESC
  LIMIT 10
)
SELECT
  _t0.nation AS NATION,
  _t0.o_year AS O_YEAR,
  _t0.amount AS AMOUNT
FROM _t0 AS _t0
ORDER BY
  _t0.ordering_1,
  _t0.ordering_2 DESC
