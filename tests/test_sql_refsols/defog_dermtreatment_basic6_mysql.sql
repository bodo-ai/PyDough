SELECT
  patient_id,
  first_name,
  last_name
FROM patients
WHERE
  EXISTS(
    SELECT
      1 AS `1`
    FROM treatments AS treatments
    JOIN outcomes AS outcomes
      ON outcomes.treatment_id = treatments.treatment_id
    WHERE
      patients.patient_id = treatments.patient_id
  )
