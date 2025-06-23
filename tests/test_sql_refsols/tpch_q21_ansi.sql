WITH _t7 AS (
  SELECT
    l_commitdate,
    l_linenumber,
    l_orderkey,
    l_receiptdate,
    l_suppkey
  FROM tpch.lineitem
  WHERE
    l_commitdate < l_receiptdate
), _t4 AS (
  SELECT
    ANY_VALUE(_t7.l_linenumber) AS anything_l_linenumber,
    ANY_VALUE(_t7.l_orderkey) AS anything_l_orderkey,
    ANY_VALUE(_t7.l_suppkey) AS anything_l_suppkey,
    ANY_VALUE(orders.o_orderkey) AS anything_o_orderkey,
    ANY_VALUE(orders.o_orderstatus) AS anything_o_orderstatus
  FROM _t7 AS _t7
  JOIN tpch.orders AS orders
    ON _t7.l_orderkey = orders.o_orderkey
  JOIN tpch.lineitem AS lineitem
    ON _t7.l_suppkey <> lineitem.l_suppkey AND lineitem.l_orderkey = orders.o_orderkey
  GROUP BY
    _t7.l_linenumber,
    _t7.l_orderkey,
    orders.o_orderkey
), _s11 AS (
  SELECT
    _t9.l_linenumber,
    _t9.l_orderkey,
    orders.o_orderkey
  FROM _t7 AS _t9
  JOIN tpch.orders AS orders
    ON _t9.l_orderkey = orders.o_orderkey
  JOIN tpch.lineitem AS lineitem
    ON _t9.l_suppkey <> lineitem.l_suppkey
    AND lineitem.l_commitdate < lineitem.l_receiptdate
    AND lineitem.l_orderkey = orders.o_orderkey
), _s13 AS (
  SELECT
    _t4.anything_l_suppkey AS agg_24,
    COUNT(*) AS n_rows
  FROM _t4 AS _t4
  JOIN _s11 AS _s11
    ON _s11.l_linenumber = _t4.anything_l_linenumber
    AND _s11.l_orderkey = _t4.anything_l_orderkey
    AND _s11.o_orderkey = _t4.anything_o_orderkey
  WHERE
    _t4.anything_o_orderstatus = 'F'
  GROUP BY
    _t4.anything_l_suppkey
)
SELECT
  supplier.s_name AS S_NAME,
  COALESCE(_s13.n_rows, 0) AS NUMWAIT
FROM tpch.supplier AS supplier
JOIN tpch.nation AS nation
  ON nation.n_name = 'SAUDI ARABIA' AND nation.n_nationkey = supplier.s_nationkey
LEFT JOIN _s13 AS _s13
  ON _s13.agg_24 = supplier.s_suppkey
ORDER BY
  numwait DESC,
  s_name
LIMIT 10
