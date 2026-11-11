SELECT
  AVG(CAST(weight_kg AS DOUBLE)) AS CAW_male
FROM cassandra.defog.patients
WHERE
  gender = 'Male'
