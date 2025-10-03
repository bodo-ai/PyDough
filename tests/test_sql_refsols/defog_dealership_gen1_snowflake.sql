SELECT
  first_name,
  last_name,
  phone,
  DATEDIFF(DAY, CAST(hire_date AS DATETIME), CAST(termination_date AS DATETIME)) AS days_employed
FROM main.salespersons
WHERE
  NOT termination_date IS NULL
ORDER BY
  4 NULLS FIRST
LIMIT 1
