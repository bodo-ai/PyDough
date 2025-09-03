SELECT
  first_name,
  last_name,
  phone,
  EXTRACT(EPOCH FROM CAST(termination_date AS TIMESTAMP) - CAST(hire_date AS TIMESTAMP)) / 86400 * 1.0 AS days_employed
FROM main.salespersons
WHERE
  NOT termination_date IS NULL
ORDER BY
  4 NULLS FIRST
LIMIT 1
