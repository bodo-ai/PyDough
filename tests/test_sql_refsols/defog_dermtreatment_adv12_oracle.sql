SELECT
  first_name,
  last_name,
  specialty
FROM MAIN.DOCTORS
WHERE
  LOWER(first_name) LIKE 'j%' OR LOWER(last_name) LIKE '%son%'
