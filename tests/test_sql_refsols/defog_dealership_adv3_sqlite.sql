WITH _s1 AS (
  SELECT
    car_id
  FROM main.sales
)
SELECT
  MAX(cars.make) AS make,
  MAX(cars.model) AS model,
  COUNT(_s1.car_id) AS num_sales
FROM main.cars AS cars
LEFT JOIN _s1 AS _s1
  ON _s1.car_id = cars._id
WHERE
  LOWER(cars.vin_number) LIKE '%m5%'
GROUP BY
  cars._id
