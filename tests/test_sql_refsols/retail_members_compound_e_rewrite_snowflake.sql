SELECT
  COUNT(*) AS n
FROM bodo.retail.protected_loyalty_members
WHERE
  PTY_UNPROTECT_DOB(date_of_birth) < CAST('1983-01-30' AS DATE)
  AND PTY_UNPROTECT_DOB(date_of_birth) >= CAST('1983-01-10' AS DATE)
