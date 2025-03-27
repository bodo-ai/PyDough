SELECT
  _id,
  first_name,
  last_name
FROM (
  SELECT
    _id,
    first_name,
    last_name
  FROM main.salespersons
)
WHERE
  NOT EXISTS(
    SELECT
      1
    FROM (
      SELECT
        salesperson_id
      FROM main.sales
    )
    WHERE
      _id = salesperson_id
  )
