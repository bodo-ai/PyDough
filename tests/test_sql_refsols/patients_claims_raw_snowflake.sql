SELECT
  COUNT(DISTINCT patient_id) AS n
FROM bodo.health.claims
WHERE
  PTY_UNPROTECT(claim_status, 'deAccount') = 'Denied'
  AND PTY_UNPROTECT(provider_name, 'deName') IN ('Smith Ltd', 'Smith Inc')
  AND YEAR(CAST(PTY_UNPROTECT_DOB(claim_date) AS TIMESTAMP)) = 2024
