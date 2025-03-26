WITH _table_alias_0 AS (
  SELECT
    supplier.s_suppkey AS key,
    supplier.s_name AS name,
    supplier.s_nationkey AS nation_key
  FROM tpch.supplier AS supplier
), _t3 AS (
  SELECT
    nation.n_name AS name,
    nation.n_nationkey AS key
  FROM tpch.nation AS nation
  WHERE
    nation.n_name = 'SAUDI ARABIA'
), _table_alias_1 AS (
  SELECT
    _t3.key AS key
  FROM _t3 AS _t3
), _table_alias_6 AS (
  SELECT
    _table_alias_0.key AS key,
    _table_alias_0.name AS name
  FROM _table_alias_0 AS _table_alias_0
  JOIN _table_alias_1 AS _table_alias_1
    ON _table_alias_0.nation_key = _table_alias_1.key
), _t5 AS (
  SELECT
    lineitem.l_commitdate AS commit_date,
    lineitem.l_orderkey AS order_key,
    lineitem.l_receiptdate AS receipt_date,
    lineitem.l_suppkey AS supplier_key
  FROM tpch.lineitem AS lineitem
  WHERE
    lineitem.l_commitdate < lineitem.l_receiptdate
), _table_alias_4 AS (
  SELECT
    _t5.supplier_key AS original_key,
    _t5.order_key AS order_key,
    _t5.supplier_key AS supplier_key
  FROM _t5 AS _t5
), _t6 AS (
  SELECT
    orders.o_orderkey AS key,
    orders.o_orderstatus AS order_status
  FROM tpch.orders AS orders
  WHERE
    orders.o_orderstatus = 'F'
), _table_alias_5 AS (
  SELECT
    _t6.key AS key
  FROM _t6 AS _t6
), _table_alias_3 AS (
  SELECT
    _table_alias_5.key AS key,
    _table_alias_4.original_key AS original_key,
    _table_alias_4.supplier_key AS supplier_key
  FROM _table_alias_4 AS _table_alias_4
  JOIN _table_alias_5 AS _table_alias_5
    ON _table_alias_4.order_key = _table_alias_5.key
), _t8 AS (
  SELECT
    lineitem.l_orderkey AS order_key,
    lineitem.l_suppkey AS supplier_key
  FROM tpch.lineitem AS lineitem
), _t7 AS (
  SELECT
    _t8.order_key AS order_key
  FROM _t8 AS _t8
  WHERE
    _t8.supplier_key <> _table_alias_3.original_key
), _table_alias_2 AS (
  SELECT
    _table_alias_3.key AS key,
    _table_alias_3.original_key AS original_key,
    _table_alias_3.supplier_key AS supplier_key
  FROM _table_alias_3 AS _table_alias_3
  WHERE
    EXISTS(
      SELECT
        1 AS "1"
      FROM _t7 AS _t7
      WHERE
        _t7.order_key = _table_alias_3.key
    )
), _t9 AS (
  SELECT
    _t10.order_key AS order_key
  FROM _t5 AS _t10
  WHERE
    _t10.supplier_key <> _table_alias_2.original_key
), _t4 AS (
  SELECT
    _table_alias_2.supplier_key AS supplier_key
  FROM _table_alias_2 AS _table_alias_2
  WHERE
    NOT EXISTS(
      SELECT
        1 AS "1"
      FROM _t9 AS _t9
      WHERE
        _t9.order_key = _table_alias_2.key
    )
), _table_alias_7 AS (
  SELECT
    COUNT() AS agg_0,
    _t4.supplier_key AS supplier_key
  FROM _t4 AS _t4
  GROUP BY
    _t4.supplier_key
), _t2 AS (
  SELECT
    _table_alias_7.agg_0 AS agg_0,
    _table_alias_6.name AS name
  FROM _table_alias_6 AS _table_alias_6
  LEFT JOIN _table_alias_7 AS _table_alias_7
    ON _table_alias_6.key = _table_alias_7.supplier_key
), _t1 AS (
  SELECT
    COALESCE(_t2.agg_0, 0) AS numwait,
    COALESCE(_t2.agg_0, 0) AS ordering_1,
    _t2.name AS s_name,
    _t2.name AS ordering_2
  FROM _t2 AS _t2
), _t0 AS (
  SELECT
    _t1.numwait AS numwait,
    _t1.s_name AS s_name,
    _t1.ordering_1 AS ordering_1,
    _t1.ordering_2 AS ordering_2
  FROM _t1 AS _t1
  ORDER BY
    ordering_1 DESC,
    ordering_2
  LIMIT 10
)
SELECT
  _t0.s_name AS S_NAME,
  _t0.numwait AS NUMWAIT
FROM _t0 AS _t0
ORDER BY
  _t0.ordering_1 DESC,
  _t0.ordering_2
