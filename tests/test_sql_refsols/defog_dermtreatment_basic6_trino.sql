WITH _u_0 AS (
  SELECT
    treatments.patient_id AS _u_1
  FROM cassandra.defog.treatments AS treatments
  JOIN postgres.main.outcomes AS outcomes
    ON outcomes.treatment_id = treatments.treatment_id
  GROUP BY
    1
)
SELECT
  patients.patient_id,
  patients.first_name,
  patients.last_name
FROM cassandra.defog.patients AS patients
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = patients.patient_id
WHERE
  NOT _u_0._u_1 IS NULL
