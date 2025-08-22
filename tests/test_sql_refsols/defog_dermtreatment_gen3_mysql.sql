SELECT
  AVG(YEAR(CURRENT_TIMESTAMP()) - YEAR(date_of_birth)) AS average_age
FROM main.patients
WHERE
  LOWER(gender) = 'male' AND LOWER(ins_type) = 'private'
