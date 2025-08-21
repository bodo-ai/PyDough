WITH _s1 AS (
  SELECT
    COUNT(*) AS n_rows,
    car_id
  FROM main.sales
  GROUP BY
    2
)
SELECT
  cars.make,
  cars.model,
  COALESCE(_s1.n_rows, 0) AS num_sales
FROM main.cars AS cars
LEFT JOIN _s1 AS _s1
  ON _s1.car_id = cars._id
WHERE
  LOWER(cars.vin_number) LIKE '%m5%'
