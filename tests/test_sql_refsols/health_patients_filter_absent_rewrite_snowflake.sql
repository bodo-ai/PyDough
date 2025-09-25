SELECT
  COUNT(*) AS n
FROM bodo.health.protected_patients
WHERE
  date_of_birth > '2003-06-29' OR date_of_birth IS NULL
