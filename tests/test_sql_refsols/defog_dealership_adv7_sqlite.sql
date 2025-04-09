WITH _s1 AS (
  SELECT
    AVG(sale_price) AS agg_0,
    car_id
  FROM main.sales
  GROUP BY
    car_id
)
SELECT
  cars.make,
  cars.model,
  cars.year,
  cars.color,
  cars.vin_number,
  _s1.agg_0 AS avg_sale_price
FROM main.cars AS cars
LEFT JOIN _s1 AS _s1
  ON _s1.car_id = cars._id
WHERE
  LOWER(cars.make) LIKE '%fordS%' OR LOWER(cars.model) LIKE '%mustang%'
