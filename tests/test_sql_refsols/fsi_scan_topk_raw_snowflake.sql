SELECT
  firstname AS first_name,
  lastname AS last_name,
  city,
  zipcode,
  dob AS date_of_birth
FROM bodo.fsi.protected_customers
ORDER BY
  2 NULLS FIRST
LIMIT 5
