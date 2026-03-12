SELECT
  COUNT(*) AS n
FROM tpch.ORDERS
WHERE
  EXISTS(
    SELECT
      1 AS `1`
    FROM tpch.LINEITEM
    WHERE
      l_orderkey = ORDERS.o_orderkey AND l_quantity > 49
  )
