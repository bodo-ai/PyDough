SELECT
  o_orderdate AS order_date,
  QUARTER(CAST(o_orderdate AS TIMESTAMP)) AS quarter,
  DATE_TRUNC('QUARTER', CAST(o_orderdate AS TIMESTAMP)) AS quarter_start,
  DATE_ADD('QUARTER', 1, CAST(o_orderdate AS TIMESTAMP)) AS next_quarter,
  DATE_ADD('QUARTER', -1, CAST(o_orderdate AS TIMESTAMP)) AS prev_quarter,
  DATE_ADD('QUARTER', 2, CAST(o_orderdate AS TIMESTAMP)) AS two_quarters_ahead,
  DATE_ADD('QUARTER', -2, CAST(o_orderdate AS TIMESTAMP)) AS two_quarters_behind,
  DATE_DIFF(
    'QUARTER',
    CAST(DATE_TRUNC('QUARTER', CAST('1995-01-01' AS TIMESTAMP)) AS TIMESTAMP),
    CAST(DATE_TRUNC('QUARTER', CAST(o_orderdate AS TIMESTAMP)) AS TIMESTAMP)
  ) AS quarters_since_1995,
  DATE_DIFF(
    'QUARTER',
    CAST(DATE_TRUNC('QUARTER', CAST(o_orderdate AS TIMESTAMP)) AS TIMESTAMP),
    CAST(DATE_TRUNC('QUARTER', CAST('2000-01-01' AS TIMESTAMP)) AS TIMESTAMP)
  ) AS quarters_until_2000,
  DATE_ADD('QUARTER', -4, CAST(o_orderdate AS TIMESTAMP)) AS same_quarter_prev_year,
  DATE_ADD('QUARTER', 4, CAST(o_orderdate AS TIMESTAMP)) AS same_quarter_next_year
FROM tpch.orders
WHERE
  YEAR(CAST(o_orderdate AS TIMESTAMP)) = 1995
ORDER BY
  1 NULLS FIRST
LIMIT 1
