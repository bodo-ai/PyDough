WITH _s1 AS (
  SELECT
    l_orderkey
  FROM tpch.lineitem
  WHERE
    l_commitdate < l_receiptdate
)
SELECT
  orders.o_orderpriority AS O_ORDERPRIORITY,
  COUNT(*) AS ORDER_COUNT
FROM tpch.orders AS orders
SEMI JOIN _s1 AS _s1
  ON _s1.l_orderkey = orders.o_orderkey
WHERE
  EXTRACT(MONTH FROM CAST(orders.o_orderdate AS DATETIME)) IN (7, 8, 9)
  AND EXTRACT(YEAR FROM CAST(orders.o_orderdate AS DATETIME)) = 1993
GROUP BY
  1
ORDER BY
  1
