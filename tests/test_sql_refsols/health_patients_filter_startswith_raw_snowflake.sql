SELECT
  COUNT(*) AS n
FROM bodo.health.protected_patients
WHERE
  STARTSWITH(PTY_UNPROTECT(phone_number, 'dePhone'), '001')
