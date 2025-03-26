SELECT
  first_name,
  last_name,
  phone,
  days_employed
FROM (
  SELECT
    *
  FROM (
    SELECT
      DATEDIFF(termination_date, hire_date, DAY) * 1.0 AS days_employed,
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
  QUALIFY
    ROW_NUMBER() OVER (ORDER BY days_employed NULLS LAST) = 1
)
