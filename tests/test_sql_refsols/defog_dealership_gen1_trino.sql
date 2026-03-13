SELECT
  first_name,
  last_name,
  phone,
  DATE_DIFF('DAY', CAST(hire_date AS TIMESTAMP), CAST(termination_date AS TIMESTAMP)) AS days_employed
FROM main.salespersons
WHERE
  NOT termination_date IS NULL
ORDER BY
  4 NULLS FIRST
LIMIT 1
