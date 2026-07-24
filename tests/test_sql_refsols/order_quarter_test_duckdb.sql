SELECT
  o_orderdate AS order_date,
  EXTRACT(QUARTER FROM CAST(o_orderdate AS TIMESTAMP)) AS quarter,
  DATE_TRUNC('QUARTER', CAST(o_orderdate AS TIMESTAMP)) AS quarter_start,
  CAST(o_orderdate AS TIMESTAMP) + INTERVAL '3' MONTH AS next_quarter,
  CAST(o_orderdate AS TIMESTAMP) - INTERVAL '3' MONTH AS prev_quarter,
  CAST(o_orderdate AS TIMESTAMP) + INTERVAL '6' MONTH AS two_quarters_ahead,
  CAST(o_orderdate AS TIMESTAMP) - INTERVAL '6' MONTH AS two_quarters_behind,
  DATE_DIFF('QUARTER', CAST('1995-01-01' AS TIMESTAMP), CAST(o_orderdate AS TIMESTAMP)) AS quarters_since_1995,
  DATE_DIFF('QUARTER', CAST(o_orderdate AS TIMESTAMP), CAST('2000-01-01' AS TIMESTAMP)) AS quarters_until_2000,
  CAST(o_orderdate AS TIMESTAMP) - INTERVAL '12' MONTH AS same_quarter_prev_year,
  CAST(o_orderdate AS TIMESTAMP) + INTERVAL '12' MONTH AS same_quarter_next_year
FROM tpch.orders
WHERE
  EXTRACT(YEAR FROM CAST(o_orderdate AS TIMESTAMP)) = 1995
ORDER BY
  1 NULLS FIRST
LIMIT 1
