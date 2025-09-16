SELECT
  COUNT(*) AS num_treatments
FROM main.treatments AS treatments
JOIN main.patients AS patients
  ON LOWER(patients.first_name) = 'alice'
  AND patients.patient_id = treatments.patient_id
WHERE
  treatments.start_dt < DATE_TRUNC('MONTH', CURRENT_TIMESTAMP())
  AND treatments.start_dt >= DATE_SUB(DATE_TRUNC('MONTH', CURRENT_TIMESTAMP()), 6, MONTH)
