WITH _t1 AS (
  SELECT
    ARBITRARY(cars.make) AS anything_make,
    ARBITRARY(cars.model) AS anything_model,
    COUNT(sales.car_id) AS count_car_id
  FROM postgres.main.cars AS cars
  LEFT JOIN cassandra.defog.sales AS sales
    ON cars._id = sales.car_id
  WHERE
    LOWER(cars.vin_number) LIKE '%m5%'
  GROUP BY
    cars._id
)
SELECT
  anything_make AS make,
  anything_model AS model,
  COALESCE(SUM(count_car_id), 0) AS num_sales
FROM _t1
GROUP BY
  1,
  2
