WITH _s1 AS (
  SELECT
    car_id
  FROM main.sales
)
SELECT
  cars._id,
  cars.make,
  cars.model,
  cars.year
FROM main.cars AS cars
ANTI JOIN _s1 AS _s1
  ON _s1.car_id = cars._id
