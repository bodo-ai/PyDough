WITH _s1 AS (
  SELECT
    COUNT(*) AS n_rows,
    patient_id
  FROM main.treatments
  GROUP BY
    patient_id
)
SELECT
  patients.patient_id,
  patients.first_name,
  patients.last_name
FROM main.patients AS patients
LEFT JOIN _s1 AS _s1
  ON _s1.patient_id = patients.patient_id
WHERE
  (
    _s1.n_rows = 0 OR _s1.n_rows IS NULL
  ) = 1
