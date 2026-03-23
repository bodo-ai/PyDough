SELECT
  COUNT(*) AS num_treatments
FROM postgres.main.treatments AS treatments
JOIN postgres.main.patients AS patients
  ON LOWER(patients.first_name) = 'alice'
  AND patients.patient_id = treatments.patient_id
WHERE
  treatments.start_dt < DATE_TRUNC('MONTH', CURRENT_TIMESTAMP)
  AND treatments.start_dt >= DATE_ADD('MONTH', -6, DATE_TRUNC('MONTH', CURRENT_TIMESTAMP))
