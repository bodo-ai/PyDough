SELECT
  COUNT(*) AS n
FROM bodo.retail.protected_loyalty_members
WHERE
  CONTAINS(LOWER(last_name), 'hu')
