SELECT
  COUNT(*) AS num_treatments
FROM defog.dermtreatment.treatments AS treatments
JOIN defog.dermtreatment.patients AS patients
  ON LOWER(patients.first_name) = 'alice'
  AND patients.patient_id = treatments.patient_id
WHERE
  treatments.start_dt < TRUNC(CURRENT_TIMESTAMP(), 'MONTH')
  AND treatments.start_dt >= ADD_MONTHS(TRUNC(CURRENT_TIMESTAMP(), 'MONTH'), -6)
