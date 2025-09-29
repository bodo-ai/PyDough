SELECT
  COUNT(*) AS n
FROM bodo.health.protected_patients
WHERE
  ENDSWITH(PTY_UNPROTECT(email, 'deEmail'), 'gmail.com')
