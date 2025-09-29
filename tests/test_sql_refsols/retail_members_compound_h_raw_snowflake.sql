SELECT
  COUNT(*) AS n
FROM bodo.retail.protected_loyalty_members
WHERE
  PTY_UNPROTECT(date_of_birth, 'deDOB') < CAST('2007-01-01' AS DATE)
  AND PTY_UNPROTECT_NAME(last_name) >= 'Cross'
