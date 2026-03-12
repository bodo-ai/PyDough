SELECT
  COUNT(*) AS n
FROM tpch.CUSTOMER
WHERE
  EXISTS(
    SELECT
      1 AS `1`
    FROM tpch.ORDERS
    WHERE
      CUSTOMER.c_custkey = o_custkey AND o_totalprice > 400000
  )
