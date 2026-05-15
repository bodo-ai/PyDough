SELECT
  customers.state,
  COUNT(DISTINCT sales.customer_id) AS unique_customers,
  COALESCE(SUM(sales.sale_price), 0) AS total_revenue
FROM dealership.sales AS sales
JOIN dealership.customers AS customers
  ON customers.id = sales.customer_id
GROUP BY
  1
ORDER BY
  3 DESC NULLS LAST
LIMIT 5
