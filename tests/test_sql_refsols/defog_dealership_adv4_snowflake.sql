SELECT
  COUNT(*) AS num_sales,
  COALESCE(SUM(sale_price), 0) AS total_revenue
FROM main.sales
WHERE
  sale_date >= DATEADD(DAY, -30, CURRENT_TIMESTAMP())
GROUP BY
  car_id
