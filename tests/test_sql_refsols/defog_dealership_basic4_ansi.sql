WITH _s1 AS (
  SELECT
    sales.salesperson_id AS salesperson_id
  FROM main.sales AS sales
), _s0 AS (
  SELECT
    salespersons._id AS _id,
    salespersons.first_name AS first_name,
    salespersons.last_name AS last_name
  FROM main.salespersons AS salespersons
)
SELECT
  _s0._id AS _id,
  _s0.first_name AS first_name,
  _s0.last_name AS last_name
FROM _s0 AS _s0
WHERE
  NOT EXISTS(
    SELECT
      1 AS "1"
    FROM _s1 AS _s1
    WHERE
      _s0._id = _s1.salesperson_id
  )
