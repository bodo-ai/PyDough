SELECT
  patient_id,
  first_name,
  last_name
FROM main.patients
WHERE
  NOT EXISTS(
    SELECT
      1 AS "1"
    FROM main.treatments
    WHERE
      patients.patient_id = patient_id
  )
