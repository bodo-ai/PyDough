SELECT
  first_name,
  last_name,
  phone,
  DATE_DIFF(
    'DAY',
    CAST(DATE_TRUNC('DAY', CAST(hire_date AS TIMESTAMP)) AS TIMESTAMP),
    CAST(DATE_TRUNC('DAY', CAST(termination_date AS TIMESTAMP)) AS TIMESTAMP)
  ) AS days_employed
FROM postgres.main.salespersons
WHERE
  NOT termination_date IS NULL
ORDER BY
  4 NULLS FIRST
LIMIT 1
