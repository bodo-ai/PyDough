WITH _s3 AS (
  SELECT DISTINCT
    treatments.patient_id
  FROM main.treatments AS treatments
  JOIN main.outcomes AS outcomes
    ON outcomes.treatment_id = treatments.treatment_id
)
SELECT
  patients.patient_id,
  patients.first_name,
  patients.last_name
FROM main.patients AS patients
JOIN _s3 AS _s3
  ON _s3.patient_id = patients.patient_id
