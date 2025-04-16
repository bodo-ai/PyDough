WITH _t2 AS (
  SELECT
    SUM(sales.sale_price) AS agg_0,
    customers.state AS customer_state,
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
  LEFT JOIN main.customers AS customers
    ON customers._id = sales.customer_id
  WHERE
    CAST(STRFTIME('%Y', sales.sale_date) AS INTEGER) = 2023
  GROUP BY
    customers.state,
    DATE(
      sales.sale_date,
      'start of month',
      '-' || CAST((
        (
          CAST(STRFTIME('%m', DATETIME(sales.sale_date)) AS INTEGER) - 1
        ) % 3
      ) AS TEXT) || ' months'
    )
)
SELECT
  quarter,
  customer_state,
  COALESCE(agg_0, 0) AS total_sales
FROM _t2
WHERE
  NOT agg_0 IS NULL AND agg_0 > 0
ORDER BY
  quarter,
  customer_state
