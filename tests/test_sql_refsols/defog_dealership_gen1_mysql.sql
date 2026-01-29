SELECT
  first_name,
  last_name,
  phone,
  DATEDIFF(termination_date, hire_date) AS days_employed
FROM dealership.salespersons
WHERE
  NOT termination_date IS NULL
ORDER BY
  4
LIMIT 1
