SELECT
  patients.patient_id,
  patients.first_name,
  patients.last_name
FROM main.patients AS patients
JOIN main.treatments AS treatments
  ON patients.patient_id = treatments.patient_id
JOIN main.outcomes AS outcomes
  ON outcomes.treatment_id = treatments.treatment_id
