WITH _s1 AS (
  SELECT
    car_id,
    COUNT(*) AS n_rows
  FROM main.sales
  GROUP BY
    1
)
SELECT
  cars.make,
  cars.model,
  _s1.n_rows AS num_sales
FROM main.cars AS cars
JOIN _s1 AS _s1
  ON _s1.car_id = cars._id
WHERE
  CONTAINS(LOWER(cars.vin_number), 'm5')
