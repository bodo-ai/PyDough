SELECT
  customers.state,
  COUNT(DISTINCT sales.customer_id) AS unique_customers,
  COALESCE(SUM(sales.sale_price), 0) AS total_revenue
FROM main.sales AS sales
JOIN main.customers AS customers
  ON customers._id = sales.customer_id
GROUP BY
  customers.state
ORDER BY
  total_revenue DESC
LIMIT 5
