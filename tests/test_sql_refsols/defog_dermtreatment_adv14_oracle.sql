SELECT
  AVG(weight_kg) AS CAW_male
FROM MAIN.PATIENTS
WHERE
  gender = 'Male'
