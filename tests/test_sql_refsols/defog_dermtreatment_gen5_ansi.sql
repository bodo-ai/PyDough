SELECT
  COUNT(*) AS num_treatments
FROM main.treatments AS treatments
JOIN main.diagnoses AS diagnoses
  ON LOWER(diagnoses.diag_name) LIKE '%psoriasis%'
  AND diagnoses.diag_id = treatments.diag_id
JOIN main.drugs AS drugs
  ON NOT drugs.fda_appr_dt IS NULL AND drugs.drug_id = treatments.drug_id
WHERE
  NOT treatments.end_dt IS NULL
  AND treatments.end_dt >= DATE_TRUNC('DAY', DATE_ADD(CURRENT_TIMESTAMP(), -6, 'MONTH'))
