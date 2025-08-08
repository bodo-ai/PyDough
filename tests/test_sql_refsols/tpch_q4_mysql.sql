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
  EXTRACT(QUARTER FROM CAST(ORDERS.o_orderdate AS DATETIME)) = 3
  AND EXTRACT(YEAR FROM CAST(ORDERS.o_orderdate AS DATETIME)) = 1993
  AND NOT _u_0._u_1 IS NULL
GROUP BY
  ORDERS.o_orderpriority
ORDER BY
  ORDERS.o_orderpriority
