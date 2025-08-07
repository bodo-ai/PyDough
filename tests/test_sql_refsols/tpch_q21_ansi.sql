WITH _t3 AS (
  SELECT
    l_commitdate,
    l_orderkey,
    l_receiptdate,
    l_suppkey
  FROM tpch.lineitem
  WHERE
    l_commitdate < l_receiptdate
), _s9 AS (
  SELECT
    COUNT(*) AS n_rows,
    _t3.l_suppkey
  FROM _t3 AS _t3
  JOIN tpch.orders AS orders
    ON _t3.l_orderkey = orders.o_orderkey AND orders.o_orderstatus = 'F'
  JOIN tpch.lineitem AS lineitem
    ON _t3.l_suppkey <> lineitem.l_suppkey AND lineitem.l_orderkey = orders.o_orderkey
  JOIN _t3 AS _t5
    ON _t3.l_suppkey <> _t5.l_suppkey AND _t5.l_orderkey = orders.o_orderkey
  GROUP BY
    _t3.l_suppkey
)
SELECT
  supplier.s_name AS S_NAME,
  COALESCE(_s9.n_rows, 0) AS NUMWAIT
FROM tpch.supplier AS supplier
JOIN tpch.nation AS nation
  ON nation.n_name = 'SAUDI ARABIA' AND nation.n_nationkey = supplier.s_nationkey
LEFT JOIN _s9 AS _s9
  ON _s9.l_suppkey = supplier.s_suppkey
ORDER BY
  COALESCE(_s9.n_rows, 0) DESC,
  s_name
LIMIT 10
