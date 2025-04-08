WITH _t1_2 AS (
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
  COALESCE(_t1.agg_0, 0) AS num_sales
FROM main.cars AS cars
LEFT JOIN _t1_2 AS _t1
  ON _t1.car_id = cars._id
WHERE
  cars.vin_number LIKE '%m5%'
