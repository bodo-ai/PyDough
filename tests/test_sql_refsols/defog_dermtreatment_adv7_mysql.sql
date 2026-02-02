SELECT
  COUNT(*) AS num_treatments
FROM treatments AS treatments
JOIN patients AS patients
  ON LOWER(patients.first_name) = 'alice'
  AND patients.patient_id = treatments.patient_id
WHERE
  treatments.start_dt < STR_TO_DATE(
    CONCAT(YEAR(CURRENT_TIMESTAMP()), ' ', MONTH(CURRENT_TIMESTAMP()), ' 1'),
    '%Y %c %e'
  )
  AND treatments.start_dt >= DATE_SUB(
    STR_TO_DATE(
      CONCAT(YEAR(CURRENT_TIMESTAMP()), ' ', MONTH(CURRENT_TIMESTAMP()), ' 1'),
      '%Y %c %e'
    ),
    INTERVAL '6' MONTH
  )
