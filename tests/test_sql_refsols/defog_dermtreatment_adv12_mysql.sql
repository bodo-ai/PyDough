SELECT
  first_name,
  last_name,
  specialty
FROM doctors
WHERE
  LOWER(first_name) LIKE 'j%' OR LOWER(last_name) LIKE '%son%'
