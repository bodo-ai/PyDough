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
    MAX(_t5.l_suppkey) AS anything_l_suppkey,
    MAX(orders.o_orderstatus) AS anything_o_orderstatus
  FROM _t5 AS _t5
  JOIN tpch.orders AS orders
    ON _t5.l_orderkey = orders.o_orderkey
  JOIN tpch.lineitem AS lineitem
    ON _t5.l_suppkey <> lineitem.l_suppkey AND lineitem.l_orderkey = orders.o_orderkey
  GROUP BY
    1,
    2,
    3
), _u_0 AS (
  SELECT
    _t6.l_linenumber AS _u_1,
    _t6.l_orderkey AS _u_2,
    orders.o_orderkey AS _u_3
  FROM _t5 AS _t6
  JOIN tpch.orders AS orders
    ON _t6.l_orderkey = orders.o_orderkey
  JOIN tpch.lineitem AS lineitem
    ON _t6.l_suppkey <> lineitem.l_suppkey
    AND lineitem.l_commitdate < lineitem.l_receiptdate
    AND lineitem.l_orderkey = orders.o_orderkey
  GROUP BY
    1,
    2,
    3
), _s13 AS (
  SELECT
    _t3.anything_l_suppkey
  FROM _t3 AS _t3
  LEFT JOIN _u_0 AS _u_0
    ON _t3.l_linenumber = _u_0._u_1
    AND _t3.l_orderkey = _u_0._u_2
    AND _t3.o_orderkey = _u_0._u_3
  WHERE
    _t3.anything_o_orderstatus = 'F' AND _u_0._u_1 IS NULL
), _t0 AS (
  SELECT
    _s13.anything_l_suppkey,
    MAX(supplier.s_name) AS anything_s_name,
    COUNT(*) AS n_rows
  FROM tpch.supplier AS supplier
  JOIN tpch.nation AS nation
    ON nation.n_name = 'SAUDI ARABIA' AND nation.n_nationkey = supplier.s_nationkey
  LEFT JOIN _s13 AS _s13
    ON _s13.anything_l_suppkey = supplier.s_suppkey
  GROUP BY
    1
)
SELECT
  anything_s_name AS S_NAME,
  n_rows * CASE WHEN NOT anything_l_suppkey IS NULL THEN 1 ELSE 0 END AS NUMWAIT
FROM _t0
ORDER BY
  2 DESC NULLS LAST,
  1 NULLS FIRST
LIMIT 10
