SELECT
  first_name,
  last_name,
  phone,
  DATEDIFF(CAST(termination_date AS DATETIME), CAST(hire_date AS DATETIME), DAY) * 1.0 AS days_employed
FROM main.salespersons
WHERE
  NOT termination_date IS NULL
ORDER BY
  days_employed
LIMIT 1
