SELECT
  COUNT(*) AS n
FROM bodo.health.protected_patients
WHERE
  ENDSWITH(email, 'gmail.com')
