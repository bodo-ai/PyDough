SELECT
  COUNT(*) AS n
FROM crbnk.customers
WHERE
  EXISTS(
    SELECT
      1 AS "1"
    FROM crbnk.accounts
    WHERE
      (
        SUBSTRING(a_type, -1) || SUBSTRING(a_type, 1, LENGTH(a_type) - 1)
      ) <> 'checking'
      AND (
        SUBSTRING(a_type, -1) || SUBSTRING(a_type, 1, LENGTH(a_type) - 1)
      ) <> 'savings'
      AND a_custkey = (
        42 - customers.c_key
      )
  )
