SELECT
  COUNT(*) AS n
FROM crbnk.customers
WHERE
  EXISTS(
    SELECT
      1 AS "1"
    FROM crbnk.accounts
    WHERE
      NOT a_type IN ('avingss', 'heckingc') AND a_custkey = (
        42 - customers.c_key
      )
  )
