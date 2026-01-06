SELECT
  COUNT(*) AS n
FROM bodo.retail.protected_loyalty_members
WHERE
  NOT date_of_birth IN ('2605-10-18', '1711-07-03', '3018-03-01', '2830-08-29')
  AND PTY_UNPROTECT_NAME(last_name) >= 'Cross'
