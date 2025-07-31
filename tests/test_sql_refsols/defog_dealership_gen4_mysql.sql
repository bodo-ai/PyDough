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
    SUM(sale_price) AS sum_sale_price,
    customer_id
  FROM main.sales
  WHERE
    YEAR(sale_date) = 2023
  GROUP BY
    STR_TO_DATE(
      CONCAT(
        YEAR(CAST(sale_date AS DATETIME)),
        ' ',
        QUARTER(CAST(sale_date AS DATETIME)) * 3 - 2,
        ' 1'
      ),
      '%Y %c %e'
    ),
    customer_id
), _t1 AS (
  SELECT
    SUM(_s0.sum_sale_price) AS sum_sum_sale_price,
    _s0.quarter,
    customers.state
  FROM _s0 AS _s0
  JOIN main.customers AS customers
    ON _s0.customer_id = customers._id
  GROUP BY
    _s0.quarter,
    customers.state
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
