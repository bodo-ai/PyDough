SELECT
  COUNT(*) AS n
FROM bodo.fsi.protected_customers
WHERE
  NOT lastname IN ('Barnes', 'Hernandez', 'Moore')
