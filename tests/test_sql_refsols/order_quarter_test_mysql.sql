SELECT
  o_orderdate AS order_date,
  EXTRACT(QUARTER FROM CAST(o_orderdate AS DATETIME)) AS quarter,
  STR_TO_DATE(
    CONCAT(
      YEAR(CAST(o_orderdate AS DATETIME)),
      ' ',
      QUARTER(CAST(o_orderdate AS DATETIME)) * 3 - 2,
      ' 1'
    ),
    '%Y %c %e'
  ) AS quarter_start,
  DATE_ADD(CAST(o_orderdate AS DATETIME), INTERVAL '1' QUARTER) AS next_quarter,
  DATE_SUB(CAST(o_orderdate AS DATETIME), INTERVAL '1' QUARTER) AS prev_quarter,
  DATE_ADD(CAST(o_orderdate AS DATETIME), INTERVAL '2' QUARTER) AS two_quarters_ahead,
  DATE_SUB(CAST(o_orderdate AS DATETIME), INTERVAL '2' QUARTER) AS two_quarters_behind,
  (
    YEAR(o_orderdate) - YEAR(CAST('1995-01-01' AS DATETIME))
  ) * 4 + (
    QUARTER(o_orderdate) - QUARTER(CAST('1995-01-01' AS DATETIME))
  ) AS quarters_since_1995,
  (
    YEAR(CAST('2000-01-01' AS DATETIME)) - YEAR(o_orderdate)
  ) * 4 + (
    QUARTER(CAST('2000-01-01' AS DATETIME)) - QUARTER(o_orderdate)
  ) AS quarters_until_2000,
  DATE_SUB(CAST(o_orderdate AS DATETIME), INTERVAL '4' QUARTER) AS same_quarter_prev_year,
  DATE_ADD(CAST(o_orderdate AS DATETIME), INTERVAL '4' QUARTER) AS same_quarter_next_year
FROM tpch.ORDERS
WHERE
  EXTRACT(YEAR FROM CAST(o_orderdate AS DATETIME)) = 1995
ORDER BY
  1
LIMIT 1
