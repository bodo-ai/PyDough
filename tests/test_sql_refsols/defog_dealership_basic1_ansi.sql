WITH _t1 AS (
  SELECT
    sales.car_id AS car_id
  FROM main.sales AS sales
), _t0 AS (
  SELECT
    cars._id AS _id,
    cars.make AS make,
    cars.model AS model,
    cars.year AS year
  FROM main.cars AS cars
)
SELECT
  _t0._id AS _id,
  _t0.make AS make,
  _t0.model AS model,
  _t0.year AS year
FROM _t0 AS _t0
WHERE
  NOT EXISTS(
    SELECT
      1 AS "1"
    FROM _t1 AS _t1
    WHERE
      _t0._id = _t1.car_id
  )
