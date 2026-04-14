SELECT
  COUNT(*) AS n
FROM bodo.health.protected_patients
WHERE
  UPPER(PTY_UNPROTECT_NAME(first_name)) < 'STEEVE'
  OR UPPER(PTY_UNPROTECT_NAME(first_name)) > 'TIM'
