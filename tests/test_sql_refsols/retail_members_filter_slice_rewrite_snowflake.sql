SELECT
  COUNT(*) AS n
FROM bodo.retail.protected_loyalty_members
WHERE
  SUBSTRING(PTY_UNPROTECT(first_name, 'deName'), 2, 1) = 'a'
