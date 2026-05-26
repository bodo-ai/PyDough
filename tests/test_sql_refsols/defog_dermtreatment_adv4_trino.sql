SELECT
  COUNT(DISTINCT treatments.patient_id) AS patient_count
FROM cassandra.defog.treatments AS treatments
JOIN mongo.defog.diagnoses AS diagnoses
  ON LOWER(diagnoses.diag_name) = 'psoriasis vulgaris'
  AND diagnoses.diag_id = treatments.diag_id
JOIN postgres.main.drugs AS drugs
  ON LOWER(drugs.drug_type) = 'biologic' AND drugs.drug_id = treatments.drug_id
