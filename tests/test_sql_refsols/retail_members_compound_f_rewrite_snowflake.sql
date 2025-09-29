SELECT
  COUNT(*) AS n
FROM bodo.retail.protected_loyalty_members
WHERE
  date_of_birth <= CAST('1976-07-28' AS DATE)
  AND date_of_birth > CAST('1976-07-01' AS DATE)
