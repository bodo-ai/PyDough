SELECT
  COUNT(*) AS n
FROM bodo.fsi.protected_customers
WHERE
  PTY_UNPROTECT(lastname, 'name') IN ('Barnes', 'Hernandez', 'Moore')
