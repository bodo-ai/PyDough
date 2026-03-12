SELECT
  patient_id,
  first_name,
  last_name
FROM dermtreatment.patients
WHERE
  EXISTS(
    SELECT
      1 AS "1"
    FROM dermtreatment.treatments AS treatments
    JOIN dermtreatment.outcomes AS outcomes
      ON outcomes.treatment_id = treatments.treatment_id
    WHERE
      patients.patient_id = treatments.patient_id
  )
