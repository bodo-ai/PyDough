SELECT
  claim_id AS key,
  patient_id AS patient_key,
  claim_date,
  provider_name,
  diagnosis_code,
  procedure_code,
  claim_amount,
  approved_amount,
  claim_status
FROM bodo.health.claims
ORDER BY
  7 DESC NULLS LAST
LIMIT 2
