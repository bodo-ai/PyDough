WITH _t1 AS (
  SELECT
    sales.salesperson_id AS salesperson_id
  FROM main.sales AS sales
), _t0 AS (
  SELECT
    salespersons._id AS _id,
    salespersons.first_name AS first_name,
    salespersons.last_name AS last_name
  FROM main.salespersons AS salespersons
)
SELECT
  _t0._id AS _id,
  _t0.first_name AS first_name,
  _t0.last_name AS last_name
FROM _t0 AS _t0
WHERE
  NOT EXISTS(
    SELECT
      1 AS "1"
    FROM _t1 AS _t1
    WHERE
      _t0._id = _t1.salesperson_id
  )
