SELECT
  COUNT(*) AS num_sales,
  CASE WHEN COUNT(*) <> 0 THEN COALESCE(SUM(sales.sale_price), 0) ELSE NULL END AS total_revenue
FROM postgres.cars AS cars
JOIN postgres.sales AS sales
  ON cars._id = sales.car_id
  AND sales.sale_date >= DATE_ADD('DAY', -30, CURRENT_TIMESTAMP)
WHERE
  LOWER(cars.make) LIKE '%toyota%'
