WITH _s1 AS (
  SELECT
    car_id
  FROM main.sales
), _t0 AS (
  SELECT
    MAX(cars.make) AS anything_make,
    MAX(cars.model) AS anything_model,
    COUNT(_s1.car_id) AS count_car_id
  FROM main.cars AS cars
  LEFT JOIN _s1 AS _s1
    ON _s1.car_id = cars._id
  WHERE
    LOWER(cars.vin_number) LIKE '%m5%'
  GROUP BY
    cars._id
)
SELECT
  anything_make AS make,
  anything_model AS model,
  SUM(count_car_id) AS num_sales
FROM _t0
GROUP BY
  1,
  2
