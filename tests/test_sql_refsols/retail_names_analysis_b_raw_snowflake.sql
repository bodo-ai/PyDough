SELECT
  COUNT(*) AS n
FROM bodo.retail.protected_loyalty_members
WHERE
  CONTAINS('day', LOWER(SUBSTRING(PTY_UNPROTECT(first_name, 'deName'), 1, 2)))
