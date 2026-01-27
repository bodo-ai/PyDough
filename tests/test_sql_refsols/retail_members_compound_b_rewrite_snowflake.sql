SELECT
  COUNT(*) AS n
FROM bodo.retail.protected_loyalty_members
WHERE
  date_of_birth = PTY_PROTECT(CAST('1979-03-07' AS DATE), 'deDOB')
  AND last_name <> PTY_PROTECT('Smith', 'deName')
