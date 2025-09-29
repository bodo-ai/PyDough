SELECT
  COUNT(*) AS n
FROM bodo.retail.protected_loyalty_members
WHERE
  email LIKE '%.%@%mail%'
