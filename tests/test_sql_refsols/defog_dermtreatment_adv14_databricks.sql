SELECT
  AVG(weight_kg) AS CAW_male
FROM defog.dermtreatment.patients
WHERE
  gender = 'Male'
