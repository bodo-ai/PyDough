WITH _t0 AS (
  SELECT
    s_suppkey AS key,
    s_name AS name,
    s_nationkey AS nation_key
  FROM tpch.supplier
), _t3 AS (
  SELECT
    n_name AS name,
    n_nationkey AS key
  FROM tpch.nation
  WHERE
    n_name = 'SAUDI ARABIA'
), _t1 AS (
  SELECT
    key AS key
  FROM _t3
), _t8 AS (
  SELECT
    _t0.key AS key,
    _t0.name AS name
  FROM _t0 AS _t0
  JOIN _t1 AS _t1
    ON _t0.nation_key = _t1.key
), _t5 AS (
  SELECT
    l_commitdate AS commit_date,
    l_orderkey AS order_key,
    l_receiptdate AS receipt_date,
    l_suppkey AS supplier_key
  FROM tpch.lineitem
  WHERE
    l_commitdate < l_receiptdate
), _t4 AS (
  SELECT
    supplier_key AS original_key,
    order_key AS order_key,
    supplier_key AS supplier_key
  FROM _t5
), _t6 AS (
  SELECT
    o_orderkey AS key,
    o_orderstatus AS order_status
  FROM tpch.orders
  WHERE
    o_orderstatus = 'F'
), _t5_2 AS (
  SELECT
    key AS key
  FROM _t6
), _t3_2 AS (
  SELECT
    _t5.key AS key,
    _t4.original_key AS original_key,
    _t4.supplier_key AS supplier_key
  FROM _t4 AS _t4
  JOIN _t5_2 AS _t5
    ON _t4.order_key = _t5.key
), _t7 AS (
  SELECT
    l_orderkey AS order_key,
    l_suppkey AS supplier_key
  FROM tpch.lineitem
), _t6_2 AS (
  SELECT
    order_key AS order_key
  FROM _t7
  WHERE
    original_key <> supplier_key
), _t2 AS (
  SELECT
    _t3.key AS key,
    _t3.original_key AS original_key,
    _t3.supplier_key AS supplier_key
  FROM _t3_2 AS _t3
  WHERE
    EXISTS(
      SELECT
        1 AS "1"
      FROM _t6_2 AS _t6
      WHERE
        _t3.key = _t6.order_key
    )
), _t7_2 AS (
  SELECT
    _t8.order_key AS order_key
  FROM _t5 AS _t8
  WHERE
    _t2.original_key <> _t8.supplier_key
), _t4_2 AS (
  SELECT
    supplier_key AS supplier_key
  FROM _t2
  WHERE
    NOT EXISTS(
      SELECT
        1 AS "1"
      FROM _t7_2 AS _t7
      WHERE
        _t2.key = _t7.order_key
    )
), _t9 AS (
  SELECT
    COUNT() AS agg_0,
    _t4.supplier_key AS supplier_key
  FROM _t4_2 AS _t4
  GROUP BY
    _t4.supplier_key
), _t2_2 AS (
  SELECT
    _t9.agg_0 AS agg_0,
    _t8.name AS name
  FROM _t8 AS _t8
  LEFT JOIN _t9 AS _t9
    ON _t8.key = _t9.supplier_key
), _t1_2 AS (
  SELECT
    COALESCE(_t2.agg_0, 0) AS numwait,
    COALESCE(_t2.agg_0, 0) AS ordering_1,
    _t2.name AS s_name,
    _t2.name AS ordering_2
  FROM _t2_2 AS _t2
), _t0_2 AS (
  SELECT
    _t1.numwait AS numwait,
    _t1.s_name AS s_name,
    _t1.ordering_1 AS ordering_1,
    _t1.ordering_2 AS ordering_2
  FROM _t1_2 AS _t1
  ORDER BY
    ordering_1 DESC,
    ordering_2
  LIMIT 10
)
SELECT
  _t0.s_name AS S_NAME,
  _t0.numwait AS NUMWAIT
FROM _t0_2 AS _t0
ORDER BY
  _t0.ordering_1 DESC,
  _t0.ordering_2
