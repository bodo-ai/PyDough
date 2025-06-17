WITH _s0 AS (
  SELECT
    supplier.s_suppkey AS key,
    supplier.s_name AS name,
    supplier.s_nationkey AS nation_key
  FROM tpch.supplier AS supplier
), _t2 AS (
  SELECT
    nation.n_nationkey AS key,
    nation.n_name AS name
  FROM tpch.nation AS nation
), _s1 AS (
  SELECT
    _t2.key AS key
  FROM _t2 AS _t2
  WHERE
    _t2.name = 'SAUDI ARABIA'
), _s12 AS (
  SELECT
    _s0.key AS key,
    _s0.name AS name
  FROM _s0 AS _s0
  JOIN _s1 AS _s1
    ON _s0.nation_key = _s1.key
), _t7 AS (
  SELECT
    lineitem.l_commitdate AS commit_date,
    lineitem.l_linenumber AS line_number,
    lineitem.l_orderkey AS order_key,
    lineitem.l_receiptdate AS receipt_date,
    lineitem.l_suppkey AS supplier_key
  FROM tpch.lineitem AS lineitem
), _s2 AS (
  SELECT
    _t7.supplier_key AS original_key,
    _t7.line_number AS line_number,
    _t7.order_key AS order_key,
    _t7.supplier_key AS supplier_key
  FROM _t7 AS _t7
  WHERE
    _t7.commit_date < _t7.receipt_date
), _s3 AS (
  SELECT
    orders.o_orderkey AS key,
    orders.o_orderstatus AS order_status
  FROM tpch.orders AS orders
), _s4 AS (
  SELECT
    _s3.key AS key,
    _s2.line_number AS line_number,
    _s2.order_key AS order_key,
    _s3.order_status AS order_status,
    _s2.original_key AS original_key,
    _s2.supplier_key AS supplier_key
  FROM _s2 AS _s2
  JOIN _s3 AS _s3
    ON _s2.order_key = _s3.key
), _s5 AS (
  SELECT
    lineitem.l_orderkey AS order_key,
    lineitem.l_suppkey AS supplier_key
  FROM tpch.lineitem AS lineitem
), _t6 AS (
  SELECT
    _s4.order_key AS order_key,
    _s4.supplier_key AS supplier_key,
    _s5.supplier_key AS supplier_key_19,
    _s4.key AS key,
    _s4.line_number AS line_number,
    _s4.order_status AS order_status,
    _s4.original_key AS original_key
  FROM _s4 AS _s4
  JOIN _s5 AS _s5
    ON _s4.key = _s5.order_key
), _t5 AS (
  SELECT
    _t6.key AS key,
    _t6.line_number AS line_number,
    _t6.order_key AS order_key,
    _t6.order_status AS order_status,
    _t6.supplier_key AS supplier_key
  FROM _t6 AS _t6
  WHERE
    _t6.original_key <> _t6.supplier_key_19
), _t4 AS (
  SELECT
    ANY_VALUE(_t5.line_number) AS agg_13,
    ANY_VALUE(_t5.order_key) AS agg_14,
    ANY_VALUE(_t5.supplier_key) AS agg_24,
    ANY_VALUE(_t5.key) AS agg_3,
    ANY_VALUE(_t5.order_status) AS agg_6
  FROM _t5 AS _t5
  GROUP BY
    _t5.key,
    _t5.line_number,
    _t5.order_key
), _s10 AS (
  SELECT
    _t4.agg_13 AS agg_13,
    _t4.agg_14 AS agg_14,
    _t4.agg_24 AS agg_24,
    _t4.agg_3 AS agg_3
  FROM _t4 AS _t4
  WHERE
    _t4.agg_6 = 'F'
), _s6 AS (
  SELECT
    _t9.supplier_key AS original_key,
    _t9.line_number AS line_number,
    _t9.order_key AS order_key
  FROM _t7 AS _t9
  WHERE
    _t9.commit_date < _t9.receipt_date
), _s7 AS (
  SELECT
    orders.o_orderkey AS key
  FROM tpch.orders AS orders
), _s8 AS (
  SELECT
    _s7.key AS key,
    _s6.line_number AS line_number,
    _s6.order_key AS order_key,
    _s6.original_key AS original_key
  FROM _s6 AS _s6
  JOIN _s7 AS _s7
    ON _s6.order_key = _s7.key
), _t10 AS (
  SELECT
    lineitem.l_commitdate AS commit_date,
    lineitem.l_orderkey AS order_key,
    lineitem.l_receiptdate AS receipt_date,
    lineitem.l_suppkey AS supplier_key
  FROM tpch.lineitem AS lineitem
), _s9 AS (
  SELECT
    _t10.order_key AS order_key,
    _t10.supplier_key AS supplier_key
  FROM _t10 AS _t10
  WHERE
    _t10.commit_date < _t10.receipt_date
), _t8 AS (
  SELECT
    _s8.order_key AS order_key,
    _s9.supplier_key AS supplier_key_36,
    _s8.key AS key,
    _s8.line_number AS line_number,
    _s8.original_key AS original_key
  FROM _s8 AS _s8
  JOIN _s9 AS _s9
    ON _s8.key = _s9.order_key
), _s11 AS (
  SELECT
    _t8.key AS key,
    _t8.line_number AS line_number,
    _t8.order_key AS order_key
  FROM _t8 AS _t8
  WHERE
    _t8.original_key <> _t8.supplier_key_36
), _t3 AS (
  SELECT
    _s10.agg_24 AS agg_24
  FROM _s10 AS _s10
  WHERE
    NOT EXISTS(
      SELECT
        1 AS "1"
      FROM _s11 AS _s11
      WHERE
        _s10.agg_13 = _s11.line_number
        AND _s10.agg_14 = _s11.order_key
        AND _s10.agg_3 = _s11.key
    )
), _s13 AS (
  SELECT
    COUNT() AS agg_0,
    _t3.agg_24 AS agg_24
  FROM _t3 AS _t3
  GROUP BY
    _t3.agg_24
), _t1 AS (
  SELECT
    _s13.agg_0 AS agg_0,
    _s12.name AS name
  FROM _s12 AS _s12
  LEFT JOIN _s13 AS _s13
    ON _s12.key = _s13.agg_24
), _t0 AS (
  SELECT
    COALESCE(_t1.agg_0, 0) AS numwait,
    _t1.name AS s_name
  FROM _t1 AS _t1
)
SELECT
  _t0.s_name AS S_NAME,
  _t0.numwait AS NUMWAIT
FROM _t0 AS _t0
ORDER BY
  numwait DESC,
  s_name
LIMIT 10
