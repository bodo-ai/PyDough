WITH _t2 AS (
  SELECT
    SUM(sales.sale_price) AS agg_0,
    customers.state AS customer_state,
    IIF(
      CAST(STRFTIME('%m', sales.sale_date) AS INTEGER) <= 3,
      '2023-01-01',
      IIF(
        CAST(STRFTIME('%m', sales.sale_date) AS INTEGER) <= 6,
        '2023-04-01',
        IIF(CAST(STRFTIME('%m', sales.sale_date) AS INTEGER) <= 9, '2023-07-01', '2023-10-01')
      )
    ) AS quarter
  FROM main.sales AS sales
  LEFT JOIN main.customers AS customers
    ON customers._id = sales.customer_id
  WHERE
    CAST(STRFTIME('%Y', sales.sale_date) AS INTEGER) = 2023
  GROUP BY
    customers.state,
    IIF(
      CAST(STRFTIME('%m', sales.sale_date) AS INTEGER) <= 3,
      '2023-01-01',
      IIF(
        CAST(STRFTIME('%m', sales.sale_date) AS INTEGER) <= 6,
        '2023-04-01',
        IIF(CAST(STRFTIME('%m', sales.sale_date) AS INTEGER) <= 9, '2023-07-01', '2023-10-01')
      )
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
