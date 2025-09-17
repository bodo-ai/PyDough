WITH _t2 AS (
  SELECT
    DATE_TRUNC('QUARTER', CAST(sales.sale_date AS TIMESTAMP)) AS quarter,
    MAX(customers.state) AS anything_state,
    SUM(sales.sale_price) AS sum_sale_price
  FROM main.sales AS sales
  JOIN main.customers AS customers
    ON customers._id = sales.customer_id
  WHERE
    EXTRACT(YEAR FROM CAST(sales.sale_date AS TIMESTAMP)) = 2023
  GROUP BY
    sales.customer_id,
    1
), _t1 AS (
  SELECT
    anything_state,
    quarter,
    SUM(sum_sale_price) AS sum_sum_sale_price
  FROM _t2
  GROUP BY
    1,
    2
)
SELECT
  quarter,
  anything_state AS customer_state,
  sum_sum_sale_price AS total_sales
FROM _t1
WHERE
  NOT sum_sum_sale_price IS NULL AND sum_sum_sale_price > 0
ORDER BY
  1 NULLS FIRST,
  2 NULLS FIRST
