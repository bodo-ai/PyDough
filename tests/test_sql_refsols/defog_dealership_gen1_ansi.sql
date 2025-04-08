WITH _t0 AS (
  SELECT
    DATEDIFF(termination_date, hire_date, DAY) * 1.0 AS days_employed,
    first_name,
    last_name,
    DATEDIFF(termination_date, hire_date, DAY) * 1.0 AS ordering_0,
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
