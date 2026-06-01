SELECT
  first_name,
  last_name,
  specialty
FROM mongo.defog.doctors
WHERE
  LOWER(last_name) LIKE '%son%' OR STARTS_WITH(LOWER(first_name), 'j')
