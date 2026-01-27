SELECT
  COUNT(*) AS n
FROM bodo.health.protected_patients
WHERE
  CONTAINS(LOWER(PTY_UNPROTECT(last_name, 'deName')), 'e')
