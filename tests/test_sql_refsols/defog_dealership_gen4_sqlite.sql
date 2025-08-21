WITH _t2 AS (
  SELECT
    MAX(customers.state) AS state,
    SUM(sales.sale_price) AS sum_sale_price,
    DATE(
      sales.sale_date,
      'start of month',
      '-' || CAST((
        (
          CAST(STRFTIME('%m', DATETIME(sales.sale_date)) AS INTEGER) - 1
        ) % 3
      ) AS TEXT) || ' months'
    ) AS quarter
  FROM main.sales AS sales
  JOIN main.customers AS customers
    ON customers._id = sales.customer_id
  WHERE
    CAST(STRFTIME('%Y', sales.sale_date) AS INTEGER) = 2023
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
  state AS customer_state,
  COALESCE(sum_sum_sale_price, 0) AS total_sales
FROM _t1
WHERE
  NOT sum_sum_sale_price IS NULL AND sum_sum_sale_price > 0
ORDER BY
  1,
  2
