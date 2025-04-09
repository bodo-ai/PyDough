WITH _t2 AS (
  SELECT
    COUNT(DISTINCT sales.customer_id) AS agg_1,
    SUM(sales.sale_price) AS agg_0,
    customers.state
  FROM main.customers AS customers
  JOIN main.sales AS sales
    ON customers._id = sales.customer_id
  GROUP BY
    customers.state
), _t0_2 AS (
  SELECT
    COALESCE(agg_0, 0) AS ordering_2,
    state,
    COALESCE(agg_0, 0) AS total_revenue,
    agg_1 AS unique_customers
  FROM _t2
  ORDER BY
    ordering_2 DESC
  LIMIT 5
)
SELECT
  state,
  unique_customers,
  total_revenue
FROM _t0_2
ORDER BY
  ordering_2 DESC
