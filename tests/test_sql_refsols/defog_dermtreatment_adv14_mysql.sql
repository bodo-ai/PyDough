SELECT
  AVG(weight_kg) AS CAW_male
FROM patients
WHERE
  gender = 'Male'
