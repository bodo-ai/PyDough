WITH _s1 AS (
  SELECT
    patient_id
  FROM main.treatments
)
SELECT
  patients.patient_id,
  patients.first_name,
  patients.last_name
FROM main.patients AS patients
ANTI JOIN _s1 AS _s1
  ON _s1.patient_id = patients.patient_id
