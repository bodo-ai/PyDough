WITH _s1 AS (
  SELECT
    description,
    patient,
    COUNT(*) AS n_rows
  FROM synthea.conditions
  GROUP BY
    1,
    2
)
SELECT
  _s1.description AS condition_description
FROM synthea.patients AS patients
JOIN _s1 AS _s1
  ON _s1.patient = patients.patient
WHERE
  patients.ethnicity = 'italian' AND patients.gender = 'F'
GROUP BY
  1
ORDER BY
  SUM(_s1.n_rows) DESC NULLS LAST,
  1 NULLS FIRST
LIMIT 1
