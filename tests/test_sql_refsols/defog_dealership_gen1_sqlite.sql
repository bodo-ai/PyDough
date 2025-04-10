WITH _t0 AS (
  SELECT
    CAST((
      JULIANDAY(DATE(termination_date, 'start of day')) - JULIANDAY(DATE(hire_date, 'start of day'))
    ) AS INTEGER) * 1.0 AS days_employed,
    first_name,
    last_name,
    CAST((
      JULIANDAY(DATE(termination_date, 'start of day')) - JULIANDAY(DATE(hire_date, 'start of day'))
    ) AS INTEGER) * 1.0 AS ordering_0,
    phone
  FROM main.salespersons
  WHERE
    NOT termination_date IS NULL
  ORDER BY
    ordering_0
  LIMIT 1
)
SELECT
  first_name,
  last_name,
  phone,
  days_employed
FROM _t0
ORDER BY
  ordering_0
