SELECT
  COUNT(*) AS n
FROM bodo.retail.protected_loyalty_members
WHERE
  date_of_birth < CAST('1983-01-30' AS DATE)
  AND date_of_birth >= CAST('1983-01-10' AS DATE)
