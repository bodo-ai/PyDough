SELECT
  o_orderpriority AS O_ORDERPRIORITY,
  COUNT(*) AS ORDER_COUNT
FROM tpch.orders
WHERE
  EXISTS(
    SELECT
      1 AS "1"
    FROM tpch.lineitem
    WHERE
      l_commitdate < l_receiptdate AND l_orderkey = orders.o_orderkey
  )
  AND MONTH(CAST(o_orderdate AS TIMESTAMP)) IN (7, 8, 9)
  AND YEAR(CAST(o_orderdate AS TIMESTAMP)) = 1993
GROUP BY
  1
ORDER BY
  1 NULLS FIRST
