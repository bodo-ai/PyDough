SELECT
  COUNT(*) AS n
FROM bodo.health.protected_patients
WHERE
  EXISTS(
    SELECT
      1 AS "1"
    FROM bodo.health.claims
    WHERE
      YEAR(CAST(PTY_UNPROTECT_DOB(claim_date) AS TIMESTAMP)) = 2024
      AND claim_status = 'bqmCpV'
      AND patient_id = protected_patients.patient_id
      AND provider_name IN ('YkwKb baV', 'NECJT Pay')
  )
