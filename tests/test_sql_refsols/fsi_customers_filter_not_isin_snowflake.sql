SELECT
  COUNT(*) AS n
FROM bodo.fsi.customers
WHERE
  NOT lastname IN ('Barnes', 'Hernandez', 'Moore')
