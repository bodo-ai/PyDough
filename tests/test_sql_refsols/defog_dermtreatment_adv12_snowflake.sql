SELECT
  first_name,
  last_name,
  specialty
FROM main.doctors
WHERE
  CONTAINS(LOWER(last_name), 'son') OR STARTSWITH(LOWER(first_name), 'j')
