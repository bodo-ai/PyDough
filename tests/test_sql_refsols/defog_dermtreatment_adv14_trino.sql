SELECT
  AVG(weight_kg) AS CAW_male
FROM cassandra.defog.patients
WHERE
  gender = 'Male'
