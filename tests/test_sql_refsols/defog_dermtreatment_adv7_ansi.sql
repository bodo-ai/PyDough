SELECT
  COUNT(*) AS num_treatments
FROM main.patients AS patients
JOIN main.treatments AS treatments
  ON patients.patient_id = treatments.patient_id
  AND treatments.start_dt < DATE_TRUNC('MONTH', CURRENT_TIMESTAMP())
  AND treatments.start_dt >= DATE_ADD(DATE_TRUNC('MONTH', CURRENT_TIMESTAMP()), -6, 'MONTH')
WHERE
  LOWER(patients.first_name) = 'alice'
