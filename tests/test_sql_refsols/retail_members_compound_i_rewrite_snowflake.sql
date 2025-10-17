SELECT
  COUNT(*) AS n
FROM bodo.retail.protected_loyalty_members
WHERE
  SUBSTRING(UPPER(PTY_UNPROTECT_NAME(last_name)), 2, 2) IN ('UA', 'CO', 'AY', 'AL')
