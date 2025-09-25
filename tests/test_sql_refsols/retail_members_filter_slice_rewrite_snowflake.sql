SELECT
  COUNT(*) AS n
FROM bodo.retail.protected_loyalty_members
WHERE
  SUBSTRING(first_name, 2, 1) = 'a'
