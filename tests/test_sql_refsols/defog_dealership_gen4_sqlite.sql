WITH _s0 AS (
  SELECT
    DATE(
      sale_date,
      'start of month',
      '-' || CAST((
        (
          CAST(STRFTIME('%m', DATETIME(sale_date)) AS INTEGER) - 1
        ) % 3
      ) AS TEXT) || ' months'
    ) AS quarter,
    customer_id,
    SUM(sale_price) AS sum_saleprice
  FROM main.sales
  WHERE
    CAST(STRFTIME('%Y', sale_date) AS INTEGER) = 2023
  GROUP BY
    1,
    2
), _t1 AS (
  SELECT
    _s0.quarter,
    customers.state,
    SUM(_s0.sum_saleprice) AS sum_sumsaleprice
  FROM _s0 AS _s0
  JOIN main.customers AS customers
    ON _s0.customer_id = customers._id
  GROUP BY
    1,
    2
)
SELECT
  quarter,
  state AS customer_state,
  sum_sumsaleprice AS total_sales
FROM _t1
WHERE
  NOT sum_sumsaleprice IS NULL AND sum_sumsaleprice > 0
ORDER BY
  1,
  2
