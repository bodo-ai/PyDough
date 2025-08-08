SELECT
  first_name,
  last_name,
  phone,
  DATEDIFF(termination_date, hire_date) * 1.0 AS days_employed
FROM main.salespersons
WHERE
  NOT termination_date IS NULL
ORDER BY
  DATEDIFF(termination_date, hire_date) * 1.0
LIMIT 1
