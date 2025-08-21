WITH _t2 AS (
  SELECT
    ANY_VALUE(customers.state) AS state,
    SUM(sales.sale_price) AS sum_sale_price,
    STR_TO_DATE(
      CONCAT(
        YEAR(CAST(sales.sale_date AS DATETIME)),
        ' ',
        QUARTER(CAST(sales.sale_date AS DATETIME)) * 3 - 2,
        ' 1'
      ),
      '%Y %c %e'
    ) AS quarter
  FROM main.sales AS sales
  JOIN main.customers AS customers
    ON customers._id = sales.customer_id
  WHERE
    EXTRACT(YEAR FROM CAST(sales.sale_date AS DATETIME)) = 2023
  GROUP BY
    sales.customer_id,
    3
), _t1 AS (
  SELECT
    SUM(sum_sale_price) AS sum_sum_sale_price,
    quarter,
    state
  FROM _t2
  GROUP BY
    2,
    3
)
SELECT
  quarter,
  state COLLATE utf8mb4_bin AS customer_state,
  COALESCE(sum_sum_sale_price, 0) AS total_sales
FROM _t1
WHERE
  NOT sum_sum_sale_price IS NULL AND sum_sum_sale_price > 0
ORDER BY
  1,
  2
