SELECT
  first_name,
  last_name,
  phone,
  DATE_DIFF(
    'DAY',
    CAST(DATE_TRUNC('DAY', hire_date) AS TIMESTAMP),
    CAST(DATE_TRUNC('DAY', termination_date) AS TIMESTAMP)
  ) AS days_employed
FROM postgres.main.salespersons
WHERE
  NOT termination_date IS NULL
ORDER BY
  4 NULLS FIRST
LIMIT 1
