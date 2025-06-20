WITH _t7 AS (
  SELECT
    l_commitdate AS commit_date,
    l_linenumber AS line_number,
    l_orderkey AS order_key,
    l_receiptdate AS receipt_date,
    l_suppkey AS supplier_key
  FROM tpch.lineitem
), _t4 AS (
  SELECT
    ANY_VALUE(_t7.line_number) AS agg_13,
    ANY_VALUE(_t7.order_key) AS agg_14,
    ANY_VALUE(_t7.supplier_key) AS agg_24,
    ANY_VALUE(orders.o_orderkey) AS agg_3,
    ANY_VALUE(orders.o_orderstatus) AS agg_6
  FROM _t7 AS _t7
  JOIN tpch.orders AS orders
    ON _t7.order_key = orders.o_orderkey
  JOIN tpch.lineitem AS lineitem
    ON lineitem.l_orderkey = orders.o_orderkey
  WHERE
    _t7.commit_date < _t7.receipt_date AND _t7.supplier_key <> lineitem.l_suppkey
  GROUP BY
    orders.o_orderkey,
    _t7.line_number,
    _t7.order_key
), _s11 AS (
  SELECT
    orders.o_orderkey AS key,
    _t9.line_number,
    _t9.order_key
  FROM _t7 AS _t9
  JOIN tpch.orders AS orders
    ON _t9.order_key = orders.o_orderkey
  JOIN tpch.lineitem AS lineitem
    ON lineitem.l_commitdate < lineitem.l_receiptdate
    AND lineitem.l_orderkey = orders.o_orderkey
  WHERE
    _t9.commit_date < _t9.receipt_date AND _t9.supplier_key <> lineitem.l_suppkey
), _s13 AS (
  SELECT
    COUNT(*) AS agg_0,
    _t4.agg_24
  FROM _t4 AS _t4
  JOIN _s11 AS _s11
    ON _s11.key = _t4.agg_3
    AND _s11.line_number = _t4.agg_13
    AND _s11.order_key = _t4.agg_14
  WHERE
    _t4.agg_6 = 'F'
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
