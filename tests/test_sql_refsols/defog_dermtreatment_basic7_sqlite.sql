SELECT
  ins_type AS insurance_type,
  AVG(height_cm) AS avg_height,
  AVG(weight_kg) AS avg_weight
FROM main.patients
GROUP BY
  1
ORDER BY
  2 DESC
LIMIT 3
