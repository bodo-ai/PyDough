SELECT
  AVG(weight_kg) AS CAW_male
FROM postgres.patients
WHERE
  gender = 'Male'
