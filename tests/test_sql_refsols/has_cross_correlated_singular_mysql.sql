SELECT
  COUNT(*) AS n
FROM tpch.CUSTOMER
WHERE
  EXISTS(
    SELECT
      1 AS `1`
    FROM tpch.NATION
    WHERE
      CUSTOMER.c_nationkey = n_nationkey
  )
