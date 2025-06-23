WITH _s0 AS (
  SELECT
    SUM(sale_price) AS sum_sale_price,
    customer_id,
    DATE(
      sale_date,
      'start of month',
      '-' || CAST((
        (
          CAST(STRFTIME('%m', DATETIME(sale_date)) AS INTEGER) - 1
        ) % 3
      ) AS TEXT) || ' months'
    ) AS quarter
  FROM main.sales
  WHERE
    CAST(STRFTIME('%Y', sale_date) AS INTEGER) = 2023
  GROUP BY
    customer_id,
    DATE(
      sale_date,
      'start of month',
      '-' || CAST((
        (
          CAST(STRFTIME('%m', DATETIME(sale_date)) AS INTEGER) - 1
        ) % 3
      ) AS TEXT) || ' months'
    )
), _t1 AS (
  SELECT
    customers.state AS customer_state,
    SUM(_s0.sum_sale_price) AS sum_sum_sale_price,
    _s0.quarter
  FROM _s0 AS _s0
  JOIN main.customers AS customers
    ON _s0.customer_id = customers._id
  GROUP BY
    customers.state,
    _s0.quarter
)
SELECT
  quarter,
  customer_state,
  COALESCE(sum_sum_sale_price, 0) AS total_sales
FROM _t1
WHERE
  NOT sum_sum_sale_price IS NULL AND sum_sum_sale_price > 0
ORDER BY
  quarter,
  customer_state
