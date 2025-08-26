WITH _s1 AS (
  SELECT DISTINCT
    description AS DESCRIPTION,
    patient AS PATIENT
  FROM main.conditions
)
SELECT
  _s1.DESCRIPTION AS condition_description
FROM main.patients AS patients
JOIN _s1 AS _s1
  ON _s1.PATIENT = patients.patient
WHERE
  patients.ethnicity = 'american' AND patients.gender = 'F'
GROUP BY
  1
ORDER BY
  COUNT(*) DESC
LIMIT 1
