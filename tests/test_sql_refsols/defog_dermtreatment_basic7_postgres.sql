SELECT
  ins_type AS insurance_type,
  AVG(CAST(height_cm AS DECIMAL)) AS avg_height,
  AVG(CAST(weight_kg AS DECIMAL)) AS avg_weight
FROM main.patients
GROUP BY
  1
ORDER BY
  2 DESC NULLS LAST
LIMIT 3
