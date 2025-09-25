SELECT
  PTY_UNPROTECT(firstname, 'name') AS first_name,
  PTY_UNPROTECT(lastname, 'name') AS last_name,
  PTY_UNPROTECT_ADDRESS(city) AS city,
  zipcode,
  PTY_UNPROTECT(dob, 'dob') AS date_of_birth
FROM bodo.fsi.protected_customers
ORDER BY
  2 NULLS FIRST
LIMIT 5
