SELECT
  COUNT(*) AS n
FROM bodo.health.protected_patients
WHERE
  UPPER(PTY_UNPROTECT_NAME(first_name)) <= 'KIM'
  AND UPPER(PTY_UNPROTECT_NAME(first_name)) >= 'JULIA'
