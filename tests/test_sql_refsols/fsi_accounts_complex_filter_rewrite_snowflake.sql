SELECT
  COUNT(*) AS n
FROM bodo.fsi.accounts
WHERE
  NOT DAY(CAST(PTY_UNPROTECT_DOB(createddate) AS TIMESTAMP)) IN (10, 15, 20)
  AND PTY_UNPROTECT_DOB(createddate) <= '2022-09-15'
  AND PTY_UNPROTECT_DOB(createddate) >= '2022-01-20'
  AND accounttype <> PTY_PROTECT('Checking', 'deAccount')
  AND currency = PTY_PROTECT('USD', 'deAccount')
  AND status = PTY_PROTECT('Active', 'deAccount')
