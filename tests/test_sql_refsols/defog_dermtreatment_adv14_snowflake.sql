SELECT
  AVG(weight_kg) AS CAW_male
FROM dermtreatment.patients
WHERE
  gender = 'Male'
