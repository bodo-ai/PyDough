WITH _u_0 AS (
  SELECT
    treatments.patient_id AS _u_1
  FROM postgres.treatments AS treatments
  JOIN postgres.diagnoses AS diagnoses
    ON LOWER(diagnoses.diag_name) = 'psoriasis vulgaris'
    AND diagnoses.diag_id = treatments.diag_id
  JOIN postgres.drugs AS drugs
    ON LOWER(drugs.drug_type) = 'biologic' AND drugs.drug_id = treatments.drug_id
  GROUP BY
    1
)
SELECT
  COUNT(*) AS patient_count
FROM postgres.patients AS patients
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = patients.patient_id
WHERE
  NOT _u_0._u_1 IS NULL
