SELECT
  COUNT(*) AS num_sales,
  COALESCE(SUM(sale_price), 0) AS total_revenue
FROM main.sales
WHERE
  sale_date >= DATETIME('now', '-30 day')
GROUP BY
  car_id
