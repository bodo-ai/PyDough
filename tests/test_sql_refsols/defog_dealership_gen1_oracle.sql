SELECT
  first_name,
  last_name,
  phone,
  DATEDIFF(CAST(termination_date AS DATETIME), CAST(hire_date AS DATETIME), DAY) AS days_employed
FROM MAIN.SALESPERSONS
WHERE
  NOT termination_date IS NULL
ORDER BY
  4 NULLS FIRST
FETCH FIRST 1 ROWS ONLY
