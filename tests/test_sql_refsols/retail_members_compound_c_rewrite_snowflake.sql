SELECT
  COUNT(*) AS n
FROM bodo.retail.protected_loyalty_members
WHERE
  date_of_birth > CAST('1995-12-22' AS DATE) AND last_name < 'Cross'
