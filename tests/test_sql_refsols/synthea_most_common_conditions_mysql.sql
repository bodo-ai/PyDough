WITH _s1 AS (
  SELECT
    description AS DESCRIPTION,
    patient AS PATIENT,
    COUNT(*) AS n_rows
  FROM synthea.conditions
  GROUP BY
    1,
    2
)
SELECT
  _s1.DESCRIPTION COLLATE utf8mb4_bin AS condition_description
FROM synthea.patients AS patients
JOIN _s1 AS _s1
  ON _s1.PATIENT = patients.patient
WHERE
  patients.ethnicity = 'italian' AND patients.gender = 'F'
GROUP BY
  1
ORDER BY
  SUM(1 * _s1.n_rows) DESC,
  1
LIMIT 1
