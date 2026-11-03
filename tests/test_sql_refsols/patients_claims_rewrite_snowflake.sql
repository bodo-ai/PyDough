SELECT
  COUNT(DISTINCT patient_id) AS n
FROM bodo.health.claims
WHERE
  YEAR(CAST(PTY_UNPROTECT_DOB(claim_date) AS TIMESTAMP)) = 2024
  AND claim_status = 'bqmCpV'
  AND provider_name IN ('YkwKb baV', 'NECJT Pay')
