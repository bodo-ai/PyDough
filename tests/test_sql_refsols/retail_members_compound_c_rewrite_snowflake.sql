SELECT
  COUNT(*) AS n
FROM bodo.retail.protected_loyalty_members
WHERE
  PTY_UNPROTECT_DOB(date_of_birth) > CAST('1995-12-22' AS DATE)
  AND PTY_UNPROTECT_NAME(last_name) < 'Cross'
