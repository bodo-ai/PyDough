SELECT
  claim_id AS key,
  PTY_UNPROTECT(patient_id, 'account') AS patient_key,
  PTY_UNPROTECT_DOB(claim_date) AS claim_date,
  PTY_UNPROTECT(provider_name, 'name') AS provider_name,
  diagnosis_code,
  procedure_code,
  claim_amount,
  approved_amount,
  PTY_UNPROTECT(claim_status, 'account') AS claim_status
FROM bodo.health.claims
ORDER BY
  7 DESC NULLS LAST
LIMIT 2
