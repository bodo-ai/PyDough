SELECT
  COUNT(*) AS n
FROM tpch.customer
WHERE
  EXISTS(
    SELECT
      1 AS "1"
    FROM tpch.nation
    WHERE
      customer.c_nationkey = n_nationkey
  )
