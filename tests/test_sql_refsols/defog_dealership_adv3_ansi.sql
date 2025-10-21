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
    LOWER(cars.vin_number) LIKE '%m5%'
  GROUP BY
    1
)
SELECT
  anything_make AS make,
  anything_model AS model,
  n_rows * CASE WHEN NOT car_id IS NULL THEN 1 ELSE 0 END AS num_sales
FROM _t0
