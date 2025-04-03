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
ANTI JOIN (
  SELECT
    salesperson_id
  FROM main.sales
)
  ON _id = salesperson_id
