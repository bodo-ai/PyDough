SELECT
  COUNT(*) AS n
FROM bodo.fsi.protected_customers
WHERE
  NOT PTY_UNPROTECT(lastname, 'name') IN ('Barnes', 'Hernandez', 'Moore')
