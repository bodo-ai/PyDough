SELECT
  first_name,
  last_name,
  phone,
  CAST((
    JULIANDAY(DATE(termination_date, 'start of day')) - JULIANDAY(DATE(hire_date, 'start of day'))
  ) AS INTEGER) * 1.0 AS days_employed
FROM main.salespersons
WHERE
  NOT termination_date IS NULL
ORDER BY
  4
LIMIT 1
