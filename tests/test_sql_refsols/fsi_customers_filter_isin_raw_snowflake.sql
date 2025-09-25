SELECT
  COUNT(*) AS n
FROM bodo.fsi.protected_customers
WHERE
  lastname IN ('Barnes', 'Hernandez', 'Moore')
