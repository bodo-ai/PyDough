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
    MAX(_t7.l_linenumber) AS agg_13,
    MAX(_t7.l_orderkey) AS agg_14,
    MAX(_t7.l_suppkey) AS agg_24,
    MAX(orders.o_orderkey) AS agg_3,
    MAX(orders.o_orderstatus) AS agg_6
  FROM _t7 AS _t7
  JOIN tpch.orders AS orders
    ON _t7.l_orderkey = orders.o_orderkey
  JOIN tpch.lineitem AS lineitem
    ON _t7.l_suppkey <> lineitem.l_suppkey AND lineitem.l_orderkey = orders.o_orderkey
  GROUP BY
    _t7.l_linenumber,
    _t7.l_orderkey,
    orders.o_orderkey
), _u_0 AS (
  SELECT
    _t9.l_linenumber AS _u_1,
    _t9.l_orderkey AS _u_2,
    orders.o_orderkey AS _u_3
  FROM _t7 AS _t9
  JOIN tpch.orders AS orders
    ON _t9.l_orderkey = orders.o_orderkey
  JOIN tpch.lineitem AS lineitem
    ON _t9.l_suppkey <> lineitem.l_suppkey
    AND lineitem.l_commitdate < lineitem.l_receiptdate
    AND lineitem.l_orderkey = orders.o_orderkey
  GROUP BY
    _t9.l_linenumber,
    _t9.l_orderkey,
    orders.o_orderkey
), _s13 AS (
  SELECT
    COUNT(*) AS agg_0,
    _t4.agg_24
  FROM _t4 AS _t4
  LEFT JOIN _u_0 AS _u_0
    ON _t4.agg_13 = _u_0._u_1 AND _t4.agg_14 = _u_0._u_2 AND _t4.agg_3 = _u_0._u_3
  WHERE
    _t4.agg_6 = 'F' AND _u_0._u_1 IS NULL
  GROUP BY
    _t4.agg_24
)
SELECT
  supplier.s_name AS S_NAME,
  COALESCE(_s13.agg_0, 0) AS NUMWAIT
FROM tpch.supplier AS supplier
JOIN tpch.nation AS nation
  ON nation.n_name = 'SAUDI ARABIA' AND nation.n_nationkey = supplier.s_nationkey
LEFT JOIN _s13 AS _s13
  ON _s13.agg_24 = supplier.s_suppkey
ORDER BY
  numwait DESC,
  s_name
LIMIT 10
