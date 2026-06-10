SELECT
  first_name,
  last_name,
  phone,
  DATEDIFF(DAY, CAST(hire_date AS DATE), CAST(termination_date AS DATE)) AS days_employed
FROM main.salespersons
WHERE
  NOT termination_date IS NULL
ORDER BY
  4
LIMIT 1
