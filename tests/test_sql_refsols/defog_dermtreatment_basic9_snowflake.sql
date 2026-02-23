WITH _u_0 AS (
  SELECT
    patient_id AS _u_1
  FROM dermtreatment.treatments
  GROUP BY
    1
)
SELECT
  patients.patient_id,
  patients.first_name,
  patients.last_name
FROM dermtreatment.patients AS patients
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = patients.patient_id
WHERE
  _u_0._u_1 IS NULL
