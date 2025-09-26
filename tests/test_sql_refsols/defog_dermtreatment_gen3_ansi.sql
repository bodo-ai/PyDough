SELECT
  AVG(DATEDIFF(CURRENT_TIMESTAMP(), CAST(date_of_birth AS DATETIME), YEAR)) AS average_age
FROM main.patients
WHERE
  gender = 'Male' AND ins_type = 'private'
