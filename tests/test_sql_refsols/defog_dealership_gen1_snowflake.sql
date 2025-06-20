SELECT
  first_name,
  last_name,
  phone,
  DATEDIFF(DAY, hire_date, termination_date) * 1.0 AS days_employed
FROM MAIN.SALESPERSONS
WHERE
  NOT termination_date IS NULL
ORDER BY
  DAYS_EMPLOYED NULLS FIRST
LIMIT 1
