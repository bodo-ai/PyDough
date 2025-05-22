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
  WHERE
    nation.n_name = 'SAUDI ARABIA'
), _s1 AS (
  SELECT
    _t2.key AS key
  FROM _t2 AS _t2
), _s8 AS (
  SELECT
    _s0.key AS key,
    _s0.name AS name
  FROM _s0 AS _s0
  JOIN _s1 AS _s1
    ON _s0.nation_key = _s1.key
), _t4 AS (
  SELECT
    lineitem.l_commitdate AS commit_date,
    lineitem.l_orderkey AS order_key,
    lineitem.l_receiptdate AS receipt_date,
    lineitem.l_suppkey AS supplier_key
  FROM tpch.lineitem AS lineitem
  WHERE
    lineitem.l_commitdate < lineitem.l_receiptdate
), _s4 AS (
  SELECT
    _t4.supplier_key AS original_key,
    _t4.order_key AS order_key,
    _t4.supplier_key AS supplier_key
  FROM _t4 AS _t4
), _t5 AS (
  SELECT
    orders.o_orderkey AS key,
    orders.o_orderstatus AS order_status
  FROM tpch.orders AS orders
  WHERE
    orders.o_orderstatus = 'F'
), _s5 AS (
  SELECT
    _t5.key AS key
  FROM _t5 AS _t5
), _s3 AS (
  SELECT
    _s5.key AS key,
    _s4.original_key AS original_key,
    _s4.supplier_key AS supplier_key
  FROM _s4 AS _s4
  JOIN _s5 AS _s5
    ON _s4.order_key = _s5.key
), _t6 AS (
  SELECT
    lineitem.l_orderkey AS order_key,
    lineitem.l_suppkey AS supplier_key
  FROM tpch.lineitem AS lineitem
), _s6 AS (
  SELECT
    _t6.order_key AS order_key
  FROM _t6 AS _t6
  WHERE
    _s3.original_key <> _t6.supplier_key
), _s2 AS (
  SELECT
    _s3.key AS key,
    _s3.original_key AS original_key,
    _s3.supplier_key AS supplier_key
  FROM _s3 AS _s3
  WHERE
    EXISTS(
      SELECT
        1 AS "1"
      FROM _s6 AS _s6
      WHERE
        _s3.key = _s6.order_key
    )
), _s7 AS (
  SELECT
    _t7.order_key AS order_key
  FROM _t4 AS _t7
  WHERE
    _s2.original_key <> _t7.supplier_key
), _t3 AS (
  SELECT
    _s2.supplier_key AS supplier_key
  FROM _s2 AS _s2
  WHERE
    NOT EXISTS(
      SELECT
        1 AS "1"
      FROM _s7 AS _s7
      WHERE
        _s2.key = _s7.order_key
    )
), _s9 AS (
  SELECT
    COUNT() AS agg_0,
    _t3.supplier_key AS supplier_key
  FROM _t3 AS _t3
  GROUP BY
    _t3.supplier_key
), _t1 AS (
  SELECT
    _s9.agg_0 AS agg_0,
    _s8.name AS name
  FROM _s8 AS _s8
  LEFT JOIN _s9 AS _s9
    ON _s8.key = _s9.supplier_key
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
