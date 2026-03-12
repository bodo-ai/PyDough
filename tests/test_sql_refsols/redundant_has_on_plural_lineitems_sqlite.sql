SELECT
  COUNT(*) AS n
FROM tpch.orders
WHERE
  EXISTS(
    SELECT
      1 AS "1"
    FROM tpch.lineitem
    WHERE
      l_orderkey = orders.o_orderkey AND l_quantity > 49
  )
