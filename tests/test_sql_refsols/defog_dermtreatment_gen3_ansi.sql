SELECT
  AVG(DATEDIFF(CURRENT_TIMESTAMP(), CAST(date_of_birth AS DATETIME), YEAR)) AS average_age
FROM main.patients
WHERE
  LOWER(gender) = 'male' AND LOWER(ins_type) = 'private'
