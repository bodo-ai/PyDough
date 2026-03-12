SELECT
  COUNT(*) AS n
FROM crbnk.customers
WHERE
  EXISTS(
    SELECT
      1 AS "1"
    FROM crbnk.accounts
    WHERE
      a_custkey = (
        42 - customers.c_key
      )
      AND a_type = (
        SUBSTRING('retirement', 2) || SUBSTRING('retirement', 1, 1)
      )
  )
