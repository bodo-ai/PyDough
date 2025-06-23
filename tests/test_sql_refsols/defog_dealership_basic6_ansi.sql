WITH _t0 AS (
  SELECT
    COALESCE(SUM(sales.sale_price), 0) AS total_revenue,
    COUNT(DISTINCT sales.customer_id) AS ndistinct_customer_id,
    customers.state
  FROM main.sales AS sales
  JOIN main.customers AS customers
    ON customers._id = sales.customer_id
  GROUP BY
    customers.state
)
SELECT
  state,
  ndistinct_customer_id AS unique_customers,
  total_revenue
FROM _t0
ORDER BY
  total_revenue DESC
LIMIT 5
