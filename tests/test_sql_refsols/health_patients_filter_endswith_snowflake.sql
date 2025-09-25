SELECT
  COUNT(*) AS n
FROM bodo.health.patients
WHERE
  ENDSWITH(email, 'gmail.com')
