SELECT
  ins_type AS insurance_type,
  AVG(height_cm) AS avg_height,
  AVG(weight_kg) AS avg_weight
FROM MAIN.PATIENTS
GROUP BY
  ins_type
ORDER BY
  2 DESC NULLS LAST
FETCH FIRST 3 ROWS ONLY
