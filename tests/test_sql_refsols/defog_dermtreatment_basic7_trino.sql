SELECT
  ins_type AS insurance_type,
  AVG(CAST(height_cm AS DOUBLE)) AS avg_height,
  AVG(CAST(weight_kg AS DOUBLE)) AS avg_weight
FROM cassandra.defog.patients
GROUP BY
  1
ORDER BY
  2 DESC
LIMIT 3
