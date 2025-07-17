WITH _t7 AS (
  SELECT
    l_commitdate,
    l_linenumber,
    l_orderkey,
    l_receiptdate,
    l_suppkey
  FROM tpch.LINEITEM
  WHERE
    l_commitdate < l_receiptdate
), _t4 AS (
  SELECT
    ANY_VALUE(_t7.l_linenumber) AS anything_l_linenumber,
    ANY_VALUE(_t7.l_orderkey) AS anything_l_orderkey,
    ANY_VALUE(_t7.l_suppkey) AS anything_l_suppkey,
    ANY_VALUE(ORDERS.o_orderkey) AS anything_o_orderkey,
    ANY_VALUE(ORDERS.o_orderstatus) AS anything_o_orderstatus
  FROM _t7 AS _t7
  JOIN tpch.ORDERS AS ORDERS
    ON ORDERS.o_orderkey = _t7.l_orderkey
  JOIN tpch.LINEITEM AS LINEITEM
    ON LINEITEM.l_orderkey = ORDERS.o_orderkey AND LINEITEM.l_suppkey <> _t7.l_suppkey
  GROUP BY
    _t7.l_linenumber,
    _t7.l_orderkey,
    ORDERS.o_orderkey
), _u_0 AS (
  SELECT
    _t9.l_linenumber AS _u_1,
    _t9.l_orderkey AS _u_2,
    ORDERS.o_orderkey AS _u_3
  FROM _t7 AS _t9
  JOIN tpch.ORDERS AS ORDERS
    ON ORDERS.o_orderkey = _t9.l_orderkey
  JOIN tpch.LINEITEM AS LINEITEM
    ON LINEITEM.l_commitdate < LINEITEM.l_receiptdate
    AND LINEITEM.l_orderkey = ORDERS.o_orderkey
    AND LINEITEM.l_suppkey <> _t9.l_suppkey
  GROUP BY
    _t9.l_linenumber,
    _t9.l_orderkey,
    ORDERS.o_orderkey
), _s13 AS (
  SELECT
    COUNT(*) AS n_rows,
    _t4.anything_l_suppkey
  FROM _t4 AS _t4
  LEFT JOIN _u_0 AS _u_0
    ON _t4.anything_l_linenumber = _u_0._u_1
    AND _t4.anything_l_orderkey = _u_0._u_2
    AND _t4.anything_o_orderkey = _u_0._u_3
  WHERE
    _t4.anything_o_orderstatus = 'F' AND _u_0._u_1 IS NULL
  GROUP BY
    _t4.anything_l_suppkey
)
SELECT
  SUPPLIER.s_name AS S_NAME,
  COALESCE(_s13.n_rows, 0) AS NUMWAIT
FROM tpch.SUPPLIER AS SUPPLIER
JOIN tpch.NATION AS NATION
  ON NATION.n_name = 'SAUDI ARABIA' AND NATION.n_nationkey = SUPPLIER.s_nationkey
LEFT JOIN _s13 AS _s13
  ON SUPPLIER.s_suppkey = _s13.anything_l_suppkey
ORDER BY
  NUMWAIT DESC,
  SUPPLIER.s_name
LIMIT 10
