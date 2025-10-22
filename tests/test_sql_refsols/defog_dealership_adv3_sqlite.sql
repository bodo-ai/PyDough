WITH _t0 AS (
  SELECT
    MAX(cars.make) AS anything_make,
    MAX(cars.model) AS anything_model,
    COUNT(sales.car_id) AS count_car_id
  FROM main.cars AS cars
  LEFT JOIN main.sales AS sales
    ON cars._id = sales.car_id
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
