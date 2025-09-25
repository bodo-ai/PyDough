SELECT
  COUNT(*) AS n
FROM bodo.health.protected_patients
WHERE
  PTY_UNPROTECT(date_of_birth, 'dob') > '2003-06-29'
  OR PTY_UNPROTECT(date_of_birth, 'dob') IS NULL
