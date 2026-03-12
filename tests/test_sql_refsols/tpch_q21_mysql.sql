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
), _s11 AS (
  SELECT
    anything_l_suppkey
  FROM _t3
  WHERE
    NOT EXISTS(
      SELECT
        1 AS `1`
      FROM _t5 AS _t6
      JOIN tpch.LINEITEM AS LINEITEM
        ON LINEITEM.l_commitdate < LINEITEM.l_receiptdate
        AND LINEITEM.l_orderkey = _t6.l_orderkey
        AND LINEITEM.l_suppkey <> _t6.l_suppkey
      WHERE
        _t3.l_linenumber = _t6.l_linenumber
        AND _t3.l_orderkey = _t6.l_orderkey
        AND _t3.o_orderkey = _t6.l_orderkey
    )
    AND anything_o_orderstatus = 'F'
)
SELECT
  ANY_VALUE(SUPPLIER.s_name) COLLATE utf8mb4_bin AS S_NAME,
  COUNT(_s11.anything_l_suppkey) AS NUMWAIT
FROM tpch.SUPPLIER AS SUPPLIER
JOIN tpch.NATION AS NATION
  ON NATION.n_name = 'SAUDI ARABIA' AND NATION.n_nationkey = SUPPLIER.s_nationkey
LEFT JOIN _s11 AS _s11
  ON SUPPLIER.s_suppkey = _s11.anything_l_suppkey
GROUP BY
  SUPPLIER.s_suppkey
ORDER BY
  2 DESC,
  1
LIMIT 10
