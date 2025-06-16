WITH _s0 AS (
  SELECT
    supplier.s_name AS s_name,
    supplier.s_nationkey AS s_nationkey,
    supplier.s_suppkey AS s_suppkey
  FROM tpch.supplier AS supplier
), _t2 AS (
  SELECT
    nation.n_name AS n_name,
    nation.n_nationkey AS n_nationkey
  FROM tpch.nation AS nation
), _s1 AS (
  SELECT
    _t2.n_nationkey AS n_nationkey
  FROM _t2 AS _t2
  WHERE
    _t2.n_name = 'SAUDI ARABIA'
), _s8 AS (
  SELECT
    _s0.s_name AS s_name,
    _s0.s_suppkey AS s_suppkey
  FROM _s0 AS _s0
  JOIN _s1 AS _s1
    ON _s0.s_nationkey = _s1.n_nationkey
), _t4 AS (
  SELECT
    lineitem.l_commitdate AS l_commitdate,
    lineitem.l_orderkey AS l_orderkey,
    lineitem.l_receiptdate AS l_receiptdate,
    lineitem.l_suppkey AS l_suppkey
  FROM tpch.lineitem AS lineitem
), _s4 AS (
  SELECT
    _t4.l_orderkey AS l_orderkey,
    _t4.l_suppkey AS l_suppkey
  FROM _t4 AS _t4
  WHERE
    _t4.l_commitdate < _t4.l_receiptdate
), _t5 AS (
  SELECT
    orders.o_orderkey AS o_orderkey,
    orders.o_orderstatus AS o_orderstatus
  FROM tpch.orders AS orders
), _s5 AS (
  SELECT
    _t5.o_orderkey AS o_orderkey
  FROM _t5 AS _t5
  WHERE
    _t5.o_orderstatus = 'F'
), _s3 AS (
  SELECT
    _s4.l_suppkey AS l_suppkey,
    _s5.o_orderkey AS o_orderkey
  FROM _s4 AS _s4
  JOIN _s5 AS _s5
    ON _s4.l_orderkey = _s5.o_orderkey
), _t6 AS (
  SELECT
    lineitem.l_orderkey AS l_orderkey,
    lineitem.l_suppkey AS l_suppkey
  FROM tpch.lineitem AS lineitem
), _s6 AS (
  SELECT
    _t6.l_orderkey AS l_orderkey
  FROM _t6 AS _t6
  WHERE
    _s3.l_suppkey <> _t6.l_suppkey
), _s2 AS (
  SELECT
    _s3.l_suppkey AS l_suppkey,
    _s3.o_orderkey AS o_orderkey
  FROM _s3 AS _s3
  WHERE
    EXISTS(
      SELECT
        1 AS "1"
      FROM _s6 AS _s6
      WHERE
        _s3.o_orderkey = _s6.l_orderkey
    )
), _s7 AS (
  SELECT
    _t7.l_orderkey AS l_orderkey
  FROM _t4 AS _t7
  WHERE
    _s2.l_suppkey <> _t7.l_suppkey AND _t7.l_commitdate < _t7.l_receiptdate
), _t3 AS (
  SELECT
    _s2.l_suppkey AS l_suppkey
  FROM _s2 AS _s2
  WHERE
    NOT EXISTS(
      SELECT
        1 AS "1"
      FROM _s7 AS _s7
      WHERE
        _s2.o_orderkey = _s7.l_orderkey
    )
), _s9 AS (
  SELECT
    COUNT() AS agg_0,
    _t3.l_suppkey AS supplier_key
  FROM _t3 AS _t3
  GROUP BY
    _t3.l_suppkey
), _t1 AS (
  SELECT
    _s9.agg_0 AS agg_0,
    _s8.s_name AS s_name
  FROM _s8 AS _s8
  LEFT JOIN _s9 AS _s9
    ON _s8.s_suppkey = _s9.supplier_key
), _t0 AS (
  SELECT
    COALESCE(_t1.agg_0, 0) AS numwait,
    _t1.s_name AS s_name
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
