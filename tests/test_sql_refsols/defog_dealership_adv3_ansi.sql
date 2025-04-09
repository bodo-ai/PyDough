WITH _s1 AS (
  SELECT
    COUNT() AS agg_0,
    car_id
  FROM main.sales
  GROUP BY
    car_id
)
SELECT
  cars.make,
  cars.model,
  COALESCE(_s1.agg_0, 0) AS num_sales
FROM main.cars AS cars
LEFT JOIN _s1 AS _s1
  ON _s1.car_id = cars._id
WHERE
  LOWER(cars.vin_number) LIKE '%m5%'
