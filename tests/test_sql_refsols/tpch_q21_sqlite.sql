WITH _t3 AS (
  SELECT
    l_commitdate,
    l_orderkey,
    l_receiptdate,
    l_suppkey
  FROM tpch.lineitem
  WHERE
    l_commitdate < l_receiptdate
), _u_0 AS (
  SELECT
    ARRAY_AGG(l_suppkey) AS _u_1,
    l_orderkey AS _u_2
  FROM tpch.lineitem
  GROUP BY
    l_orderkey
), _u_3 AS (
  SELECT
    ARRAY_AGG(l_suppkey) AS _u_4,
    l_orderkey AS _u_5
  FROM _t3
  GROUP BY
    l_orderkey
), _s9 AS (
  SELECT
    COUNT(*) AS n_rows,
    _t3.l_suppkey
  FROM _t3 AS _t3
  JOIN tpch.orders AS orders
    ON _t3.l_orderkey = orders.o_orderkey AND orders.o_orderstatus = 'F'
  LEFT JOIN _u_0 AS _u_0
    ON _u_0._u_2 = orders.o_orderkey
  LEFT JOIN _u_3 AS _u_3
    ON _u_3._u_5 = orders.o_orderkey
  WHERE
    ARRAY_ANY(_u_0._u_1, _x -> _t3.l_suppkey <> _x)
    AND (
      NOT ARRAY_ANY(_u_3._u_4, _x -> _t3.l_suppkey <> _x) OR _u_3._u_4 IS NULL
    )
    AND NOT _u_0._u_1 IS NULL
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
