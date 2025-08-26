WITH _s1 AS (
  SELECT DISTINCT
    description,
    patient
  FROM main.conditions
)
SELECT
  _s1.description AS condition_description
FROM main.patients AS patients
JOIN _s1 AS _s1
  ON patient = patient
WHERE
  patients.ethnicity = 'american' AND patients.gender = 'F'
GROUP BY
  1
ORDER BY
  COUNT(*) DESC
LIMIT 1
