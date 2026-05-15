SELECT
  first_name,
  last_name,
  phone,
  TRUNC(CAST(CAST(termination_date AS DATE) AS DATE), 'DD') - TRUNC(CAST(CAST(hire_date AS DATE) AS DATE), 'DD') AS days_employed
FROM MAIN.SALESPERSONS
WHERE
  NOT termination_date IS NULL
ORDER BY
  4 NULLS FIRST
FETCH FIRST 1 ROWS ONLY
