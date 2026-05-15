SELECT
  o_orderdate AS order_date,
  EXTRACT(QUARTER FROM CAST(o_orderdate AS DATETIME)) AS quarter,
  DATE_TRUNC('QUARTER', CAST(o_orderdate AS TIMESTAMP)) AS quarter_start,
  DATE_ADD(CAST(o_orderdate AS TIMESTAMP), 1, 'QUARTER') AS next_quarter,
  DATE_SUB(CAST(o_orderdate AS TIMESTAMP), 1, QUARTER) AS prev_quarter,
  DATE_ADD(CAST(o_orderdate AS TIMESTAMP), 2, 'QUARTER') AS two_quarters_ahead,
  DATE_SUB(CAST(o_orderdate AS TIMESTAMP), 2, QUARTER) AS two_quarters_behind,
  DATEDIFF(CAST(o_orderdate AS DATETIME), CAST('1995-01-01' AS TIMESTAMP), QUARTER) AS quarters_since_1995,
  DATEDIFF(CAST('2000-01-01' AS TIMESTAMP), CAST(o_orderdate AS DATETIME), QUARTER) AS quarters_until_2000,
  DATE_SUB(CAST(o_orderdate AS TIMESTAMP), 4, QUARTER) AS same_quarter_prev_year,
  DATE_ADD(CAST(o_orderdate AS TIMESTAMP), 4, 'QUARTER') AS same_quarter_next_year
FROM tpch.orders
WHERE
  EXTRACT(YEAR FROM CAST(o_orderdate AS DATETIME)) = 1995
ORDER BY
  1
LIMIT 1
