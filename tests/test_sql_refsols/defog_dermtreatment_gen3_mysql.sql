SELECT
  AVG(YEAR(CURRENT_TIMESTAMP()) - YEAR(date_of_birth)) AS average_age
FROM patients
WHERE
  gender = 'Male' AND ins_type = 'private'
