SELECT
  COUNT(*) AS n
FROM bodo.fsi.customers
WHERE
  lastname IN ('Barnes', 'Hernandez', 'Moore')
