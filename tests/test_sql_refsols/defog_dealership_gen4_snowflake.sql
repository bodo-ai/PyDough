WITH _s0 AS (
  SELECT
    SUM(sale_price) AS sum_sale_price,
    customer_id
  FROM main.sales
  WHERE
    YEAR(CAST(sale_date AS TIMESTAMP)) = 2023
  GROUP BY
    2,
    DATE_TRUNC('QUARTER', CAST(sale_date AS TIMESTAMP))
), _t1 AS (
  SELECT
    DATE_TRUNC('QUARTER', CAST(sale_date AS TIMESTAMP)) AS quarter,
    SUM(_s0.sum_sale_price) AS sum_sum_sale_price,
    customers.state
  FROM _s0 AS _s0
  JOIN main.customers AS customers
    ON _s0.customer_id = customers._id
  GROUP BY
    1,
    3
)
SELECT
  quarter,
  state AS customer_state,
  sum_sum_sale_price AS total_sales
FROM _t1
WHERE
  NOT sum_sum_sale_price IS NULL AND sum_sum_sale_price > 0
ORDER BY
  1 NULLS FIRST,
  2 NULLS FIRST
