SELECT
  AVG(weight_kg) AS CAW_male
FROM main.patients
WHERE
  gender = 'Male'
