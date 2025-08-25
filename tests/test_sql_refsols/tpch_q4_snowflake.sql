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
  NOT _u_0._u_1 IS NULL
  AND QUARTER(CAST(orders.o_orderdate AS TIMESTAMP)) = 3
  AND YEAR(CAST(orders.o_orderdate AS TIMESTAMP)) = 1993
GROUP BY
  1
ORDER BY
  1 NULLS FIRST
