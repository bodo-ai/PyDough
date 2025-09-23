SELECT
  COUNT(*) AS num_sales,
  COALESCE(SUM(sale_price), 0) AS total_revenue
FROM main.sales
WHERE
  sale_date >= CURRENT_TIMESTAMP - INTERVAL '30 DAY'
GROUP BY
  car_id
