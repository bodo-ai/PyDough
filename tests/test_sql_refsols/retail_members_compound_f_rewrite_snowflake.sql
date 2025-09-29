SELECT
  COUNT(*) AS n
FROM bodo.retail.protected_loyalty_members
WHERE
  PTY_UNPROTECT_DOB(date_of_birth) <= CAST('1976-07-28' AS DATE)
  AND PTY_UNPROTECT_DOB(date_of_birth) > CAST('1976-07-01' AS DATE)
