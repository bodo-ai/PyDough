WITH _s1 AS (
  SELECT DISTINCT
    description,
    patient
  FROM synthea.conditions
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
  COUNT(*) DESC,
  1
LIMIT 1
