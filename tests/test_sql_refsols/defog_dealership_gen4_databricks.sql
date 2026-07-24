WITH _s0 AS (
  SELECT
    TRUNC(CAST(sale_date AS TIMESTAMP), 'QUARTER') AS quarter,
    customer_id,
    SUM(sale_price) AS sum_sale_price
  FROM defog.dealership.sales
  WHERE
    EXTRACT(YEAR FROM CAST(sale_date AS TIMESTAMP)) = 2023
  GROUP BY
    1,
    2
), _t1 AS (
  SELECT
    _s0.quarter,
    customers.state,
    SUM(_s0.sum_sale_price) AS sum_sum_sale_price
  FROM _s0 AS _s0
  JOIN defog.dealership.customers AS customers
    ON _s0.customer_id = customers.id
  GROUP BY
    1,
    2
)
SELECT
  quarter,
  state AS customer_state,
  sum_sum_sale_price AS total_sales
FROM _t1
WHERE
  NOT sum_sum_sale_price IS NULL AND sum_sum_sale_price > 0
ORDER BY
  1,
  2
