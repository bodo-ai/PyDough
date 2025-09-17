SELECT
  conditions.description COLLATE utf8mb4_bin AS condition_description
FROM synthea.patients AS patients
JOIN synthea.conditions AS conditions
  ON conditions.patient = patients.patient
WHERE
  patients.ethnicity = 'italian' AND patients.gender = 'F'
GROUP BY
  1
ORDER BY
  COUNT(*) DESC,
  1
LIMIT 1
