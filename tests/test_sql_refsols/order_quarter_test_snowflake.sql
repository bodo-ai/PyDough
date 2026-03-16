SELECT
  o_orderdate AS order_date,
  QUARTER(CAST(o_orderdate AS TIMESTAMP)) AS quarter,
  DATE_TRUNC('QUARTER', CAST(o_orderdate AS TIMESTAMP)) AS quarter_start,
  DATEADD(QUARTER, 1, CAST(o_orderdate AS TIMESTAMP)) AS next_quarter,
  DATEADD(QUARTER, -1, CAST(o_orderdate AS TIMESTAMP)) AS prev_quarter,
  DATEADD(QUARTER, 2, CAST(o_orderdate AS TIMESTAMP)) AS two_quarters_ahead,
  DATEADD(QUARTER, -2, CAST(o_orderdate AS TIMESTAMP)) AS two_quarters_behind,
  DATEDIFF(QUARTER, CAST('1995-01-01' AS TIMESTAMP), CAST(o_orderdate AS DATETIME)) AS quarters_since_1995,
  DATEDIFF(QUARTER, CAST(o_orderdate AS DATETIME), CAST('2000-01-01' AS TIMESTAMP)) AS quarters_until_2000,
  DATEADD(QUARTER, -4, CAST(o_orderdate AS TIMESTAMP)) AS same_quarter_prev_year,
  DATEADD(QUARTER, 4, CAST(o_orderdate AS TIMESTAMP)) AS same_quarter_next_year
FROM tpch.orders
WHERE
  YEAR(CAST(o_orderdate AS TIMESTAMP)) = 1995
ORDER BY
  1 NULLS FIRST
LIMIT 1
