SELECT
  COUNT(*) AS n
FROM bodo.retail.protected_loyalty_members
WHERE
  PTY_UNPROTECT(date_of_birth, 'deDOB') >= CAST('2002-01-01' AS DATE)
  AND last_name IN (PTY_PROTECT('Johnson', 'deName'), PTY_PROTECT('Robinson', 'deName'))
