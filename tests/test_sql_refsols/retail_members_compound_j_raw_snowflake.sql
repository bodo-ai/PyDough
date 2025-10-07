SELECT
  COUNT(*) AS n
FROM bodo.retail.protected_loyalty_members
WHERE
  CONTAINS(LOWER(PTY_UNPROTECT_NAME(last_name)), 'hu')
