SELECT
  COUNT(*) AS num_treatments
FROM dermtreatment.treatments AS treatments
JOIN dermtreatment.diagnoses AS diagnoses
  ON CONTAINS(LOWER(diagnoses.diag_name), 'psoriasis')
  AND diagnoses.diag_id = treatments.diag_id
JOIN dermtreatment.drugs AS drugs
  ON NOT drugs.fda_appr_dt IS NULL AND drugs.drug_id = treatments.drug_id
WHERE
  NOT treatments.end_dt IS NULL
  AND treatments.end_dt >= DATE_TRUNC(
    'DAY',
    DATEADD(MONTH, -6, CAST(CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()) AS TIMESTAMPNTZ))
  )
