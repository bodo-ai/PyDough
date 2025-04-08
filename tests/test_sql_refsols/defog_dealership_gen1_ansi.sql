SELECT
  first_name,
  last_name,
  phone,
  days_employed
FROM (
  SELECT
    days_employed,
    first_name,
    last_name,
    ordering_0,
    phone
  FROM (
    SELECT
      DATEDIFF(termination_date, hire_date, DAY) * 1.0 AS days_employed,
      DATEDIFF(termination_date, hire_date, DAY) * 1.0 AS ordering_0,
      first_name,
      last_name,
      phone
    FROM (
      SELECT
        first_name,
        hire_date,
        last_name,
        phone,
        termination_date
      FROM main.salespersons
      WHERE
        NOT (
          termination_date IS NULL
        )
    )
  )
  ORDER BY
    ordering_0
  LIMIT 1
)
ORDER BY
  ordering_0
