SELECT
  COUNT(*) AS num_sales,
  CASE WHEN COUNT(*) <> 0 THEN COALESCE(SUM(sales.sale_price), 0) ELSE NULL END AS total_revenue
FROM dealership.cars AS cars
JOIN dealership.sales AS sales
  ON cars._id = sales.car_id
  AND sales.sale_date >= DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL '30' DAY)
WHERE
  LOWER(cars.make) LIKE '%toyota%'
