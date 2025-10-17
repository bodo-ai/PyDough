SELECT
  AVG(DATEDIFF(YEAR, CAST(date_of_birth AS DATETIME), CURRENT_TIMESTAMP())) AS average_age
FROM main.patients
WHERE
  gender = 'Male' AND ins_type = 'private'
