SELECT
  COUNT(*) AS n
FROM bodo.retail.protected_loyalty_members
WHERE
  CONTAINS('GT', UPPER(SUBSTRING(PTY_UNPROTECT(first_name, 'deName'), 1, 1)))
