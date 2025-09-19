SELECT
  COUNT(*) AS n
FROM bodo.retail.loyalty_members
WHERE
  email LIKE '%.%@%mail%'
