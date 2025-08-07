WITH _s9 AS (
  SELECT
    COUNT(*) AS n_rows,
    lineitem.l_suppkey
  FROM tpch.lineitem AS lineitem
  JOIN tpch.orders AS orders
    ON lineitem.l_orderkey = orders.o_orderkey AND orders.o_orderstatus = 'F'
  WHERE
    NOT _u_0._u_1 IS NULL
    AND _u_3._u_4 IS NULL
    AND lineitem.l_commitdate < lineitem.l_receiptdate
  GROUP BY
    lineitem.l_suppkey
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
