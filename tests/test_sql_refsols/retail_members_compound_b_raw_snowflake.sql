SELECT
  COUNT(*) AS n
FROM bodo.retail.protected_loyalty_members
WHERE
  date_of_birth = CAST('1979-03-07' AS DATE) AND last_name <> 'Smith'
