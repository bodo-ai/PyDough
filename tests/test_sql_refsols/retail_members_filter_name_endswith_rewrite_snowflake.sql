SELECT
  COUNT(*) AS n
FROM bodo.retail.protected_loyalty_members
WHERE
  ENDSWITH(PTY_UNPROTECT(first_name, 'name'), 'e')
  OR ENDSWITH(PTY_UNPROTECT_NAME(last_name), 'e')
