WITH _s1 AS (
  SELECT
    salesperson_id
  FROM main.sales
)
SELECT
  salespersons._id,
  salespersons.first_name,
  salespersons.last_name
FROM main.salespersons AS salespersons
ANTI JOIN _s1 AS _s1
  ON _s1.salesperson_id = salespersons._id
