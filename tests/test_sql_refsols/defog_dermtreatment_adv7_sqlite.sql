SELECT
  COUNT(*) AS num_treatments
FROM main.patients AS patients
JOIN main.treatments AS treatments
  ON patients.patient_id = treatments.patient_id
  AND treatments.start_dt < DATE('now', 'start of month')
  AND treatments.start_dt >= DATE('now', 'start of month', '-6 month')
WHERE
  LOWER(patients.first_name) = 'alice'
