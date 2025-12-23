SELECT
  COUNT(*) AS n
FROM bodo.retail.protected_loyalty_members
WHERE
  PTY_UNPROTECT_NAME(last_name) >= 'Cross' AND date_of_birth <> '2605-10-18'
