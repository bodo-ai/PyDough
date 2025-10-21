WITH _t5 AS (
  SELECT
    l_commitdate,
    l_linenumber,
    l_orderkey,
    l_receiptdate,
    l_suppkey
  FROM tpch.LINEITEM
  WHERE
    l_commitdate < l_receiptdate
), _t3 AS (
  SELECT
    _t5.l_linenumber,
    _t5.l_orderkey,
    ORDERS.o_orderkey,
    ANY_VALUE(_t5.l_suppkey) AS anything_l_suppkey,
    ANY_VALUE(ORDERS.o_orderstatus) AS anything_o_orderstatus
  FROM _t5 AS _t5
  JOIN tpch.ORDERS AS ORDERS
    ON ORDERS.o_orderkey = _t5.l_orderkey
  JOIN tpch.LINEITEM AS LINEITEM
    ON LINEITEM.l_orderkey = ORDERS.o_orderkey AND LINEITEM.l_suppkey <> _t5.l_suppkey
  GROUP BY
    1,
    2,
    3
), _u_0 AS (
  SELECT
    _t6.l_linenumber AS _u_1,
    _t6.l_orderkey AS _u_2,
    ORDERS.o_orderkey AS _u_3
  FROM _t5 AS _t6
  JOIN tpch.ORDERS AS ORDERS
    ON ORDERS.o_orderkey = _t6.l_orderkey
  JOIN tpch.LINEITEM AS LINEITEM
    ON LINEITEM.l_commitdate < LINEITEM.l_receiptdate
    AND LINEITEM.l_orderkey = ORDERS.o_orderkey
    AND LINEITEM.l_suppkey <> _t6.l_suppkey
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
)
SELECT
  ANY_VALUE(SUPPLIER.s_name) COLLATE utf8mb4_bin AS S_NAME,
  COUNT(_s13.anything_l_suppkey) AS NUMWAIT
FROM tpch.SUPPLIER AS SUPPLIER
JOIN tpch.NATION AS NATION
  ON NATION.n_name = 'SAUDI ARABIA' AND NATION.n_nationkey = SUPPLIER.s_nationkey
LEFT JOIN _s13 AS _s13
  ON SUPPLIER.s_suppkey = _s13.anything_l_suppkey
GROUP BY
  SUPPLIER.s_suppkey
ORDER BY
  2 DESC,
  1
LIMIT 10
