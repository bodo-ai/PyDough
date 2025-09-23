SELECT
  o_orderpriority AS O_ORDERPRIORITY,
  COUNT(*) AS ORDER_COUNT
FROM tpch.orders
WHERE
  CAST(STRFTIME('%Y', o_orderdate) AS INTEGER) = 1993
  AND CAST(STRFTIME('%m', o_orderdate) AS INTEGER) IN (7, 8, 9)
GROUP BY
  1
ORDER BY
  1
