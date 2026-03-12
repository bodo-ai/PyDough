SELECT
  o_orderpriority COLLATE utf8mb4_bin AS O_ORDERPRIORITY,
  COUNT(*) AS ORDER_COUNT
FROM tpch.ORDERS
WHERE
  EXISTS(
    SELECT
      1 AS `1`
    FROM tpch.LINEITEM
    WHERE
      l_commitdate < l_receiptdate AND l_orderkey = ORDERS.o_orderkey
  )
  AND EXTRACT(MONTH FROM CAST(o_orderdate AS DATETIME)) IN (7, 8, 9)
  AND EXTRACT(YEAR FROM CAST(o_orderdate AS DATETIME)) = 1993
GROUP BY
  1
ORDER BY
  1
