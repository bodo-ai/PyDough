SELECT
  patient_id,
  first_name,
  last_name
FROM dermtreatment.patients
WHERE
  NOT EXISTS(
    SELECT
      1 AS "1"
    FROM dermtreatment.treatments
    WHERE
      patients.patient_id = patient_id
  )
