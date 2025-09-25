SELECT
  COUNT(*) AS n
FROM bodo.health.protected_patients
WHERE
  STARTSWITH(phone_number, '001')
