SELECT
  COUNT(*) AS num_treatments
FROM cassandra.defog.treatments AS treatments
JOIN mongo.defog.diagnoses AS diagnoses
  ON LOWER(diagnoses.diag_name) LIKE '%psoriasis%'
  AND diagnoses.diag_id = treatments.diag_id
JOIN postgres.main.drugs AS drugs
  ON NOT drugs.fda_appr_dt IS NULL AND drugs.drug_id = treatments.drug_id
WHERE
  NOT treatments.end_dt IS NULL
  AND treatments.end_dt >= DATE_TRUNC('DAY', DATE_ADD('MONTH', -6, CURRENT_TIMESTAMP))
