WITH _t1 AS (
  SELECT
    COUNT(DISTINCT sales.customer_id) AS ndistinct_customer_id,
    SUM(sales.sale_price) AS sum_sale_price,
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
  COALESCE(sum_sale_price, 0) AS total_revenue
FROM _t1
ORDER BY
  total_revenue DESC
LIMIT 5
