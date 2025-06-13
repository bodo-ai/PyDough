WITH _t1 AS (
  SELECT
    SUM(sales.sale_price) AS agg_0,
    COUNT(DISTINCT sales.customer_id) AS agg_1,
    customers.state
  FROM main.sales AS sales
  JOIN main.customers AS customers
    ON customers._id = sales.customer_id
  GROUP BY
    customers.state
)
SELECT
  state,
  agg_1 AS unique_customers,
  COALESCE(agg_0, 0) AS total_revenue
FROM _t1
ORDER BY
  total_revenue DESC
LIMIT 5
