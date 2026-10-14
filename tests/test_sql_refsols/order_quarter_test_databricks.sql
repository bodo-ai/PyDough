SELECT
  o_orderdate AS order_date,
  EXTRACT(QUARTER FROM CAST(o_orderdate AS TIMESTAMP)) AS quarter,
  TRUNC(CAST(o_orderdate AS TIMESTAMP), 'QUARTER') AS quarter_start,
  DATE_ADD(QUARTER, 1, CAST(o_orderdate AS TIMESTAMP)) AS next_quarter,
  ADD_MONTHS(CAST(o_orderdate AS TIMESTAMP), -3) AS prev_quarter,
  DATE_ADD(QUARTER, 2, CAST(o_orderdate AS TIMESTAMP)) AS two_quarters_ahead,
  ADD_MONTHS(CAST(o_orderdate AS TIMESTAMP), -6) AS two_quarters_behind,
  (
    YEAR(o_orderdate) - YEAR(CAST('1995-01-01' AS TIMESTAMP))
  ) * 4 + QUARTER(o_orderdate) - QUARTER(CAST('1995-01-01' AS TIMESTAMP)) AS quarters_since_1995,
  (
    YEAR(CAST('2000-01-01' AS TIMESTAMP)) - YEAR(o_orderdate)
  ) * 4 + QUARTER(CAST('2000-01-01' AS TIMESTAMP)) - QUARTER(o_orderdate) AS quarters_until_2000,
  ADD_MONTHS(CAST(o_orderdate AS TIMESTAMP), -12) AS same_quarter_prev_year,
  DATE_ADD(QUARTER, 4, CAST(o_orderdate AS TIMESTAMP)) AS same_quarter_next_year
FROM tpch.orders
WHERE
  EXTRACT(YEAR FROM CAST(o_orderdate AS TIMESTAMP)) = 1995
ORDER BY
  1
LIMIT 1
