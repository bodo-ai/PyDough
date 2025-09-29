SELECT
  COUNT(*) AS n
FROM bodo.fsi.accounts
WHERE
  balance <= 9000 AND balance >= 8000
