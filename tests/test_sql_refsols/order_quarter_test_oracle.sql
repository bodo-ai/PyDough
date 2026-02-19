SELECT
  o_orderdate AS order_date,
  EXTRACT(QUARTER FROM CAST(o_orderdate AS DATE)) AS quarter,
  TRUNC(CAST(o_orderdate AS TIMESTAMP), 'QUARTER') AS quarter_start,
  CAST(o_orderdate AS TIMESTAMP) + NUMTODSINTERVAL(1, 'quarter') AS next_quarter,
  CAST(o_orderdate AS TIMESTAMP) + NUMTODSINTERVAL(1, 'quarter') AS prev_quarter,
  CAST(o_orderdate AS TIMESTAMP) + NUMTODSINTERVAL(2, 'quarter') AS two_quarters_ahead,
  CAST(o_orderdate AS TIMESTAMP) + NUMTODSINTERVAL(2, 'quarter') AS two_quarters_behind,
  (
    EXTRACT(YEAR FROM CAST(o_orderdate AS DATE)) - EXTRACT(YEAR FROM CAST(CAST('1995-01-01' AS TIMESTAMP) AS DATE))
  ) * 4 + (
    EXTRACT(QUARTER FROM CAST(o_orderdate AS DATE)) - EXTRACT(QUARTER FROM CAST(CAST('1995-01-01' AS TIMESTAMP) AS DATE))
  ) AS quarters_since_1995,
  (
    EXTRACT(YEAR FROM CAST(CAST('2000-01-01' AS TIMESTAMP) AS DATE)) - EXTRACT(YEAR FROM CAST(o_orderdate AS DATE))
  ) * 4 + (
    EXTRACT(QUARTER FROM CAST(CAST('2000-01-01' AS TIMESTAMP) AS DATE)) - EXTRACT(QUARTER FROM CAST(o_orderdate AS DATE))
  ) AS quarters_until_2000,
  CAST(o_orderdate AS TIMESTAMP) + NUMTODSINTERVAL(4, 'quarter') AS same_quarter_prev_year,
  CAST(o_orderdate AS TIMESTAMP) + NUMTODSINTERVAL(4, 'quarter') AS same_quarter_next_year
FROM TPCH.ORDERS
WHERE
  EXTRACT(YEAR FROM CAST(o_orderdate AS DATE)) = 1995
ORDER BY
  1 NULLS FIRST
FETCH FIRST 1 ROWS ONLY
