SELECT
  COUNT(*) AS n
FROM bodo.retail.protected_loyalty_members
WHERE
  date_of_birth >= CAST('2002-01-01' AS DATE)
  AND last_name IN ('Johnson', 'Robinson')
