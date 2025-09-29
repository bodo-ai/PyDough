SELECT
  COUNT(*) AS num_treatments
FROM main.treatments AS treatments
JOIN main.patients AS patients
  ON LOWER(patients.first_name) = 'alice'
  AND patients.patient_id = treatments.patient_id
WHERE
  treatments.start_dt < DATE('now', 'start of month')
  AND treatments.start_dt >= DATE('now', 'start of month', '-6 month')
