WITH _t1 AS (
  SELECT
    SUM(sales.sale_price) AS agg_0,
    customers.state AS customer_state,
    DATE_TRUNC('QUARTER', CAST(sales.sale_date AS TIMESTAMP)) AS quarter
  FROM main.sales AS sales
  LEFT JOIN main.customers AS customers
    ON customers._id = sales.customer_id
  WHERE
    EXTRACT(YEAR FROM sales.sale_date) = 2023
  GROUP BY
    customers.state,
    DATE_TRUNC('QUARTER', CAST(sales.sale_date AS TIMESTAMP))
)
SELECT
  quarter,
  customer_state,
  COALESCE(agg_0, 0) AS total_sales
FROM _t1
WHERE
  NOT agg_0 IS NULL AND agg_0 > 0
ORDER BY
  quarter,
  customer_state
