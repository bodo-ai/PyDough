WITH _s1 AS (
  SELECT
    car_id
  FROM main.sales
)
SELECT
  ANY_VALUE(cars.make) AS make,
  ANY_VALUE(cars.model) AS model,
  COUNT(*) AS num_sales
FROM main.cars AS cars
LEFT JOIN _s1 AS _s1
  ON _s1.car_id = cars._id
WHERE
  CONTAINS(LOWER(cars.vin_number), 'm5')
GROUP BY
  _s1.car_id
