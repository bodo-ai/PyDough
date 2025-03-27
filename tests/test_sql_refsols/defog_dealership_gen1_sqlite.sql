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
      *,
      ROW_NUMBER() OVER (ORDER BY days_employed) AS _w
    FROM (
      SELECT
        CAST((JULIANDAY(DATE(termination_date, 'start of day')) - JULIANDAY(DATE(hire_date, 'start of day'))) AS INTEGER) * 1.0 AS days_employed,
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
  ) AS _t
  WHERE
    _w = 1
)
