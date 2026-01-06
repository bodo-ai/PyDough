SELECT
  COUNT(*) AS n
FROM bodo.health.protected_patients
WHERE
  UPPER(PTY_UNPROTECT_NAME(first_name)) < 'BOB'
  OR UPPER(PTY_UNPROTECT_NAME(first_name)) > 'YOLANDA'
