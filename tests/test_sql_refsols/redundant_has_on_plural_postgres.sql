SELECT
  COUNT(*) AS n
FROM tpch.customer
WHERE
  EXISTS(
    SELECT
      1 AS "1"
    FROM tpch.orders
    WHERE
      customer.c_custkey = o_custkey AND o_totalprice > 400000
  )
