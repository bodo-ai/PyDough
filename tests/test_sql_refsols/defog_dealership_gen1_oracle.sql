SELECT
  first_name,
  last_name,
  phone,
  CAST(termination_date AS DATE) - CAST(hire_date AS DATE) AS days_employed
FROM MAIN.SALESPERSONS
WHERE
  NOT termination_date IS NULL
ORDER BY
  4 NULLS FIRST
FETCH FIRST 1 ROWS ONLY
