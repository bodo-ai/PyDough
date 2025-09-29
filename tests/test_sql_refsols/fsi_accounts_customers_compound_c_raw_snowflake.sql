SELECT
  COUNT(*) AS n
FROM bodo.fsi.accounts
WHERE
  (
    createddate <= '2020-03-13' OR createddate >= '2022-12-25'
  )
  AND (
    createddate <= '2023-01-15' OR createddate >= '2024-08-04'
  )
  AND (
    createddate <= '2024-11-08' OR createddate >= '2022-12-25'
  )
  AND (
    createddate <= '2024-11-08' OR createddate >= '2025-06-07'
  )
  AND createddate <= '2026-03-07'
  AND createddate >= '2020-01-31'
