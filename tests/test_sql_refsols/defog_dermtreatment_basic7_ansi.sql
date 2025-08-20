SELECT
  ins_type AS insurance_type,
  AVG(height_cm) AS avg_height,
  AVG(weight_kg) AS avg_weight
FROM main.patients
GROUP BY
  ins_type
ORDER BY
  avg_height DESC
LIMIT 3
