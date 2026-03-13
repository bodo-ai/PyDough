SELECT
  AVG(DATE_DIFF('YEAR', CAST(date_of_birth AS TIMESTAMP), CURRENT_TIMESTAMP)) AS average_age
FROM main.patients
WHERE
  gender = 'Male' AND ins_type = 'private'
