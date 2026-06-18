SELECT
  conditions.description AS condition_description
FROM main.patients AS patients
JOIN main.conditions AS conditions
  ON conditions.patient = patients.patient
WHERE
  patients.ethnicity = 'italian' AND patients.gender = 'F'
GROUP BY
  1
ORDER BY
  COUNT(*) DESC,
  1
LIMIT 1
