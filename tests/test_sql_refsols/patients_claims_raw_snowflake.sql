SELECT
  COUNT(*) AS n
FROM bodo.health.protected_patients
WHERE
  EXISTS(
    SELECT
      1 AS "1"
    FROM bodo.health.claims
    WHERE
      PTY_UNPROTECT(claim_status, 'deAccount') = 'Denied'
      AND PTY_UNPROTECT(provider_name, 'deName') IN ('Smith Ltd', 'Smith Inc')
      AND YEAR(CAST(PTY_UNPROTECT_DOB(claim_date) AS TIMESTAMP)) = 2024
      AND patient_id = protected_patients.patient_id
  )
