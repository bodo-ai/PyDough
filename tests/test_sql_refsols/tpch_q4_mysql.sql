WITH _u_0 AS (
  SELECT
    l_orderkey AS _u_1
  FROM tpch.LINEITEM
  WHERE
    l_commitdate < l_receiptdate
  GROUP BY
    l_orderkey
)
SELECT
  ORDERS.o_orderpriority AS O_ORDERPRIORITY,
  COUNT(*) AS ORDER_COUNT
FROM tpch.ORDERS AS ORDERS
LEFT JOIN _u_0 AS _u_0
  ON ORDERS.o_orderkey = _u_0._u_1
WHERE
  NOT _u_0._u_1 IS NULL
  AND QUARTER(ORDERS.o_orderdate) = 3
  AND YEAR(ORDERS.o_orderdate) = 1993
GROUP BY
  ORDERS.o_orderpriority
ORDER BY
  ORDERS.o_orderpriority
