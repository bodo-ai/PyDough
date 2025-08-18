WITH _t2 AS (
  SELECT
    ANY_VALUE(customers.state) AS state,
    SUM(sales.sale_price) AS sum_sale_price,
    DATE_TRUNC('QUARTER', CAST(sales.sale_date AS TIMESTAMP)) AS quarter
  FROM main.sales AS sales
  JOIN main.customers AS customers
    ON customers._id = sales.customer_id
  WHERE
    EXTRACT(YEAR FROM CAST(sales.sale_date AS DATETIME)) = 2023
  GROUP BY
    sales.customer_id,
    DATE_TRUNC('QUARTER', CAST(sales.sale_date AS TIMESTAMP))
), _t1 AS (
  SELECT
    SUM(sum_sale_price) AS sum_sum_sale_price,
    quarter,
    state
  FROM _t2
  GROUP BY
    quarter,
    state
)
SELECT
  quarter,
  state AS customer_state,
  COALESCE(sum_sum_sale_price, 0) AS total_sales
FROM _t1
WHERE
  NOT sum_sum_sale_price IS NULL AND sum_sum_sale_price > 0
ORDER BY
  quarter,
  state
