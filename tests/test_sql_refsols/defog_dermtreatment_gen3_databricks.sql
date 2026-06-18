SELECT
  AVG(YEAR(TO_DATE(CURRENT_TIMESTAMP())) - YEAR(TO_DATE(date_of_birth))) AS average_age
FROM main.patients
WHERE
  gender = 'Male' AND ins_type = 'private'
