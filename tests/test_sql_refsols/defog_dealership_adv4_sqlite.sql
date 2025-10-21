WITH _s1 AS (
  SELECT
    car_id,
    sale_price
  FROM main.sales
  WHERE
    sale_date >= DATETIME('now', '-30 day')
)
SELECT
  COUNT(_s1.car_id) AS num_sales,
  CASE WHEN COUNT(_s1.car_id) > 0 THEN COALESCE(SUM(_s1.sale_price), 0) ELSE NULL END AS total_revenue
FROM main.cars AS cars
LEFT JOIN _s1 AS _s1
  ON _s1.car_id = cars._id
WHERE
  LOWER(cars.make) LIKE '%toyota%'
GROUP BY
  cars._id
