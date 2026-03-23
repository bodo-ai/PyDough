SELECT
  AVG(weight_kg) AS CAW_male
FROM postgres.main.patients
WHERE
  gender = 'Male'
