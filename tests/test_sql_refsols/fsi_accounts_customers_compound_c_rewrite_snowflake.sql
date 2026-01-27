SELECT
  COUNT(*) AS n
FROM bodo.fsi.accounts
WHERE
  (
    PTY_UNPROTECT_DOB(createddate) <= '2020-03-13'
    OR PTY_UNPROTECT_DOB(createddate) >= '2022-12-25'
  )
  AND (
    PTY_UNPROTECT_DOB(createddate) <= '2023-01-15'
    OR PTY_UNPROTECT_DOB(createddate) >= '2024-08-04'
  )
  AND (
    PTY_UNPROTECT_DOB(createddate) <= '2024-11-08'
    OR PTY_UNPROTECT_DOB(createddate) >= '2022-12-25'
  )
  AND (
    PTY_UNPROTECT_DOB(createddate) <= '2024-11-08'
    OR PTY_UNPROTECT_DOB(createddate) >= '2025-06-07'
  )
  AND PTY_UNPROTECT_DOB(createddate) <= '2026-03-07'
  AND PTY_UNPROTECT_DOB(createddate) >= '2020-01-31'
