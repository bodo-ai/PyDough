WITH _t5 AS (
  SELECT
    l_commitdate,
    l_linenumber,
    l_orderkey,
    l_receiptdate,
    l_suppkey
  FROM tpch.lineitem
  WHERE
    l_commitdate < l_receiptdate
), _t3 AS (
  SELECT
    _t5.l_linenumber,
    _t5.l_orderkey,
    orders.o_orderkey,
    ANY_VALUE(_t5.l_suppkey) AS anything_l_suppkey,
    ANY_VALUE(orders.o_orderstatus) AS anything_o_orderstatus
  FROM _t5 AS _t5
  JOIN tpch.orders AS orders
    ON _t5.l_orderkey = orders.o_orderkey
  JOIN tpch.lineitem AS lineitem
    ON _t5.l_suppkey <> lineitem.l_suppkey AND lineitem.l_orderkey = orders.o_orderkey
  GROUP BY
    1,
    2,
    3
), _s11 AS (
  SELECT
    _t6.l_linenumber,
    _t6.l_orderkey,
    orders.o_orderkey
  FROM _t5 AS _t6
  JOIN tpch.orders AS orders
    ON _t6.l_orderkey = orders.o_orderkey
  JOIN tpch.lineitem AS lineitem
    ON _t6.l_suppkey <> lineitem.l_suppkey
    AND lineitem.l_commitdate < lineitem.l_receiptdate
    AND lineitem.l_orderkey = orders.o_orderkey
), _s13 AS (
  SELECT
    _t3.anything_l_suppkey
  FROM _t3 AS _t3
  JOIN _s11 AS _s11
    ON _s11.l_linenumber = _t3.l_linenumber
    AND _s11.l_orderkey = _t3.l_orderkey
    AND _s11.o_orderkey = _t3.o_orderkey
  WHERE
    _t3.anything_o_orderstatus = 'F'
)
SELECT
  ANY_VALUE(supplier.s_name) AS S_NAME,
  COUNT(_s13.anything_l_suppkey) AS NUMWAIT
FROM tpch.supplier AS supplier
JOIN tpch.nation AS nation
  ON nation.n_name = 'SAUDI ARABIA' AND nation.n_nationkey = supplier.s_nationkey
LEFT JOIN _s13 AS _s13
  ON _s13.anything_l_suppkey = supplier.s_suppkey
GROUP BY
  supplier.s_suppkey
ORDER BY
  2 DESC,
  1
LIMIT 10
