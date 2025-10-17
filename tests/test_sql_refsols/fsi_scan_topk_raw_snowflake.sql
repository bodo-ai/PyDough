SELECT
  PTY_UNPROTECT(firstname, 'deName') AS first_name,
  PTY_UNPROTECT(lastname, 'deName') AS last_name,
  PTY_UNPROTECT_ADDRESS(city) AS city,
  zipcode,
  PTY_UNPROTECT(dob, 'deDOB') AS date_of_birth
FROM bodo.fsi.protected_customers
ORDER BY
  2 NULLS FIRST
LIMIT 5
