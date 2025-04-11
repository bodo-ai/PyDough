WITH _s1 AS (
  SELECT
    sales.car_id AS car_id
  FROM main.sales AS sales
), _s0 AS (
  SELECT
    cars._id AS _id,
    cars.make AS make,
    cars.model AS model,
    cars.year AS year
  FROM main.cars AS cars
)
SELECT
  _s0._id AS _id,
  _s0.make AS make,
  _s0.model AS model,
  _s0.year AS year
FROM _s0 AS _s0
WHERE
  NOT EXISTS(
    SELECT
      1 AS "1"
    FROM _s1 AS _s1
    WHERE
      _s0._id = _s1.car_id
  )
