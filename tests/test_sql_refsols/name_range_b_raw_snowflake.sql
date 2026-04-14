SELECT
  COUNT(*) AS n
FROM bodo.health.protected_patients
WHERE
  (
    UPPER(PTY_UNPROTECT_NAME(first_name)) < 'KIM'
    OR UPPER(PTY_UNPROTECT_NAME(first_name)) > 'LARRY'
  )
  AND UPPER(PTY_UNPROTECT_NAME(first_name)) <= 'MATTHEW'
  AND UPPER(PTY_UNPROTECT_NAME(first_name)) >= 'JULIA'
