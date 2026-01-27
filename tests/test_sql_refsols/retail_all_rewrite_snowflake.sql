SELECT
  COUNT(*) AS n
FROM bodo.retail.protected_loyalty_members
WHERE
  YEAR(CAST(PTY_UNPROTECT(date_of_birth, 'deDOB') AS TIMESTAMP)) < 2026
