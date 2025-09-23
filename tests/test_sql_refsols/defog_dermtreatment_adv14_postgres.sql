SELECT
  AVG(CAST(weight_kg AS DECIMAL)) AS CAW_male
FROM main.patients
WHERE
  gender = 'Male'
