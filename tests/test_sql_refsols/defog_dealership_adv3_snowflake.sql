WITH _s1 AS (
  SELECT
    car_id
  FROM main.sales
), _t0 AS (
  SELECT
    _s1.car_id,
    ANY_VALUE(cars.make) AS anything_make,
    ANY_VALUE(cars.model) AS anything_model,
    COUNT(*) AS n_rows
  FROM main.cars AS cars
  LEFT JOIN _s1 AS _s1
    ON _s1.car_id = cars._id
  WHERE
    CONTAINS(LOWER(cars.vin_number), 'm5')
  GROUP BY
    1
)
SELECT
  anything_make AS make,
  anything_model AS model,
  n_rows * IFF(NOT car_id IS NULL, 1, 0) AS num_sales
FROM _t0
