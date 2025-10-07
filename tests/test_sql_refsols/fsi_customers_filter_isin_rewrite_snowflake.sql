SELECT
  COUNT(*) AS n
FROM bodo.fsi.protected_customers
WHERE
  lastname IN (PTY_PROTECT_NAME('Barnes'), PTY_PROTECT_NAME('Hernandez'), PTY_PROTECT_NAME('Moore'))
