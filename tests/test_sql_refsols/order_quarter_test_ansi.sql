WITH _t0 AS (
  SELECT
    o_orderdate AS order_date
  FROM tpch.orders
  WHERE
    EXTRACT(YEAR FROM o_orderdate) = 1995
  ORDER BY
    order_date
  LIMIT 1
)
SELECT
  order_date,
  EXTRACT(QUARTER FROM order_date) AS quarter,
  DATE_TRUNC('QUARTER', CAST(order_date AS TIMESTAMP)) AS quarter_start,
  DATE_ADD(CAST(order_date AS TIMESTAMP), 1, 'QUARTER') AS next_quarter,
  DATE_ADD(CAST(order_date AS TIMESTAMP), -1, 'QUARTER') AS prev_quarter,
  DATE_ADD(CAST(order_date AS TIMESTAMP), 2, 'QUARTER') AS two_quarters_ahead,
  DATE_ADD(CAST(order_date AS TIMESTAMP), -2, 'QUARTER') AS two_quarters_behind,
  DATEDIFF(order_date, CAST('1995-01-01' AS TIMESTAMP), QUARTER) AS quarters_since_1995,
  DATEDIFF(CAST('2000-01-01' AS TIMESTAMP), order_date, QUARTER) AS quarters_until_2000,
  DATE_ADD(CAST(order_date AS TIMESTAMP), -4, 'QUARTER') AS same_quarter_prev_year,
  DATE_ADD(CAST(order_date AS TIMESTAMP), 4, 'QUARTER') AS same_quarter_next_year
FROM _t0
ORDER BY
  order_date
