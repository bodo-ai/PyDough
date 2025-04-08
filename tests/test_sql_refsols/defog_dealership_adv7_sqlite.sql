WITH _t1 AS (
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
  _t1.agg_0 AS avg_sale_price
FROM main.cars AS cars
LEFT JOIN _t1 AS _t1
  ON _t1.car_id = cars._id
WHERE
  LOWER(cars.make) LIKE '%fordS%' OR LOWER(cars.model) LIKE '%mustang%'
