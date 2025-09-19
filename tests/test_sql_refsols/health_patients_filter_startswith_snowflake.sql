SELECT
  COUNT(*) AS n
FROM bodo.health.patients
WHERE
  STARTSWITH(phone_number, '001')
