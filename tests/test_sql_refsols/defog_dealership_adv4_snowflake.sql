WITH _s1 AS (
  SELECT
    car_id,
    sale_price
  FROM main.sales
  WHERE
    sale_date >= DATEADD(DAY, -30, CURRENT_TIMESTAMP())
)
SELECT
  COUNT(*) AS num_sales,
  COALESCE(SUM(_s1.sale_price), 0) AS total_revenue
FROM main.cars AS cars
LEFT JOIN _s1 AS _s1
  ON _s1.car_id = cars._id
WHERE
  CONTAINS(LOWER(cars.make), 'toyota')
GROUP BY
  _s1.car_id
