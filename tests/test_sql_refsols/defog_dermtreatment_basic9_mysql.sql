SELECT
  patient_id,
  first_name,
  last_name
FROM patients
WHERE
  NOT EXISTS(
    SELECT
      1 AS `1`
    FROM treatments
    WHERE
      patients.patient_id = patient_id
  )
