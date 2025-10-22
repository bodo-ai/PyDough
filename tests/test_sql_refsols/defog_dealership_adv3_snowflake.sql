WITH _t1 AS (
  SELECT
    ANY_VALUE(cars.make) AS anything_make,
    ANY_VALUE(cars.model) AS anything_model,
    COUNT(sales.car_id) AS count_car_id
  FROM main.cars AS cars
  LEFT JOIN main.sales AS sales
    ON cars._id = sales.car_id
  WHERE
    CONTAINS(LOWER(cars.vin_number), 'm5')
  GROUP BY
    cars._id
)
SELECT
  anything_make AS make,
  anything_model AS model,
  COALESCE(SUM(NULLIF(count_car_id, 0)), 0) AS num_sales
FROM _t1
GROUP BY
  1,
  2
