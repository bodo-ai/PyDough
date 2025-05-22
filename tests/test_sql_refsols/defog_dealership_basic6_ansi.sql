WITH _t1 AS (
  SELECT
    SUM(sales.sale_price) AS agg_0,
    COUNT(DISTINCT sales.customer_id) AS agg_1,
    customers.state
  FROM main.customers AS customers
  JOIN main.sales AS sales
    ON customers._id = sales.customer_id
  GROUP BY
    customers.state
)
SELECT
  state,
  COALESCE(agg_1, 0) AS unique_customers,
  COALESCE(agg_0, 0) AS total_revenue
FROM _t1
ORDER BY
  total_revenue DESC
LIMIT 5
