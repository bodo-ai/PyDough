WITH _s0 AS (
  SELECT
    STR_TO_DATE(
      CONCAT(
        YEAR(CAST(sale_date AS DATETIME)),
        ' ',
        QUARTER(CAST(sale_date AS DATETIME)) * 3 - 2,
        ' 1'
      ),
      '%Y %c %e'
    ) AS quarter,
    customer_id,
    SUM(sale_price) AS sum_sale_price
  FROM main.sales
  WHERE
    EXTRACT(YEAR FROM CAST(sale_date AS DATETIME)) = 2023
  GROUP BY
    1,
    2
), _t1 AS (
  SELECT
    _s0.quarter,
    customers.state,
    SUM(_s0.sum_sale_price) AS sum_sum_sale_price
  FROM _s0 AS _s0
  JOIN main.customers AS customers
    ON _s0.customer_id = customers._id
  GROUP BY
    1,
    2
)
SELECT
  quarter,
  state COLLATE utf8mb4_bin AS customer_state,
  sum_sum_sale_price AS total_sales
FROM _t1
WHERE
  NOT sum_sum_sale_price IS NULL AND sum_sum_sale_price > 0
ORDER BY
  1,
  2
