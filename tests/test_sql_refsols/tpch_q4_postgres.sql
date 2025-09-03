WITH _u_0 AS (
  SELECT
    l_orderkey AS _u_1
  FROM tpch.lineitem
  WHERE
    l_commitdate < l_receiptdate
  GROUP BY
    1
)
SELECT
  orders.o_orderpriority AS O_ORDERPRIORITY,
  COUNT(*) AS ORDER_COUNT
FROM tpch.orders AS orders
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = orders.o_orderkey
WHERE
  EXTRACT(QUARTER FROM CAST(orders.o_orderdate AS TIMESTAMP)) = 3
  AND EXTRACT(YEAR FROM CAST(orders.o_orderdate AS TIMESTAMP)) = 1993
  AND NOT _u_0._u_1 IS NULL
GROUP BY
  1
ORDER BY
  1 NULLS FIRST
