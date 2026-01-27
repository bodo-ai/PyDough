SELECT
  COUNT(*) AS num_sales,
  CASE WHEN COUNT(*) <> 0 THEN COALESCE(SUM(sales.sale_price), 0) ELSE NULL END AS total_revenue
FROM dealership.cars AS cars
JOIN dealership.sales AS sales
  ON cars.id = sales.car_id
  AND sales.sale_date >= DATEADD(DAY, -30, CAST(CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()) AS TIMESTAMPNTZ))
WHERE
  CONTAINS(LOWER(cars.make), 'toyota')
