SELECT
  COUNT(*) AS num_sales,
  CASE WHEN COUNT(*) <> 0 THEN COALESCE(SUM(sales.sale_price), 0) ELSE NULL END AS total_revenue
FROM main.cars AS cars
JOIN main.sales AS sales
  ON cars._id = sales.car_id AND sales.sale_date >= DATETIME('now', '-30 day')
WHERE
  LOWER(cars.make) LIKE '%toyota%'
