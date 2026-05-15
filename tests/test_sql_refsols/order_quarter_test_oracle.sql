SELECT
  o_orderdate AS order_date,
  FLOOR((
    EXTRACT(MONTH FROM CAST(o_orderdate AS DATE)) - 1
  ) / 3) + 1 AS quarter,
  TRUNC(CAST(CAST(o_orderdate AS DATE) AS DATE), 'Q') AS quarter_start,
  CAST(o_orderdate AS DATE) + NUMTOYMINTERVAL(3, 'MONTH') AS next_quarter,
  CAST(o_orderdate AS DATE) - NUMTOYMINTERVAL(3, 'MONTH') AS prev_quarter,
  CAST(o_orderdate AS DATE) + NUMTOYMINTERVAL(6, 'MONTH') AS two_quarters_ahead,
  CAST(o_orderdate AS DATE) - NUMTOYMINTERVAL(6, 'MONTH') AS two_quarters_behind,
  (
    EXTRACT(YEAR FROM CAST(o_orderdate AS DATE)) - EXTRACT(YEAR FROM TO_DATE('1995-01-01', 'YYYY-MM-DD'))
  ) * 4 + (
    FLOOR((
      EXTRACT(MONTH FROM CAST(o_orderdate AS DATE)) - 1
    ) / 3) - FLOOR((
      EXTRACT(MONTH FROM TO_DATE('1995-01-01', 'YYYY-MM-DD')) - 1
    ) / 3)
  ) AS quarters_since_1995,
  (
    EXTRACT(YEAR FROM TO_DATE('2000-01-01', 'YYYY-MM-DD')) - EXTRACT(YEAR FROM CAST(o_orderdate AS DATE))
  ) * 4 + (
    FLOOR((
      EXTRACT(MONTH FROM TO_DATE('2000-01-01', 'YYYY-MM-DD')) - 1
    ) / 3) - FLOOR((
      EXTRACT(MONTH FROM CAST(o_orderdate AS DATE)) - 1
    ) / 3)
  ) AS quarters_until_2000,
  CAST(o_orderdate AS DATE) - NUMTOYMINTERVAL(12, 'MONTH') AS same_quarter_prev_year,
  CAST(o_orderdate AS DATE) + NUMTOYMINTERVAL(12, 'MONTH') AS same_quarter_next_year
FROM TPCH.ORDERS
WHERE
  EXTRACT(YEAR FROM CAST(o_orderdate AS DATE)) = 1995
ORDER BY
  1 NULLS FIRST
FETCH FIRST 1 ROWS ONLY
