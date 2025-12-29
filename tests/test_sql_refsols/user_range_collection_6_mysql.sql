WITH _s5 AS (
  SELECT
    EXTRACT(YEAR FROM CAST(ORDERS.o_orderdate AS DATETIME)) AS year_o_orderdate,
    COUNT(DISTINCT ORDERS.o_custkey) AS ndistinct_o_custkey
  FROM tpch.ORDERS AS ORDERS
  JOIN tpch.CUSTOMER AS CUSTOMER
    ON CUSTOMER.c_custkey = ORDERS.o_custkey AND CUSTOMER.c_mktsegment = 'AUTOMOBILE'
  JOIN tpch.NATION AS NATION
    ON CUSTOMER.c_nationkey = NATION.n_nationkey AND NATION.n_name = 'JAPAN'
  WHERE
    ORDERS.o_clerk = 'Clerk#000000925'
  GROUP BY
    1
)
SELECT
  years.year,
  COALESCE(_s5.ndistinct_o_custkey, 0) AS n_orders
FROM (VALUES
  ROW(1990),
  ROW(1991),
  ROW(1992),
  ROW(1993),
  ROW(1994),
  ROW(1995),
  ROW(1996),
  ROW(1997),
  ROW(1998),
  ROW(1999),
  ROW(2000)) AS years(year)
LEFT JOIN _s5 AS _s5
  ON _s5.year_o_orderdate = years.year
ORDER BY
  1
