WITH _s5 AS (
  SELECT
    EXTRACT(YEAR FROM CAST(orders.o_orderdate AS TIMESTAMP)) AS year_o_orderdate,
    COUNT(DISTINCT orders.o_custkey) AS ndistinct_o_custkey
  FROM tpch.orders AS orders
  JOIN tpch.customer AS customer
    ON customer.c_custkey = orders.o_custkey AND customer.c_mktsegment = 'AUTOMOBILE'
  JOIN tpch.nation AS nation
    ON customer.c_nationkey = nation.n_nationkey AND nation.n_name = 'JAPAN'
  WHERE
    orders.o_clerk = 'Clerk#000000925'
  GROUP BY
    1
)
SELECT
  column1 AS year,
  COALESCE(_s5.ndistinct_o_custkey, 0) AS n_orders
FROM (VALUES
  (1990),
  (1991),
  (1992),
  (1993),
  (1994),
  (1995),
  (1996),
  (1997),
  (1998),
  (1999),
  (2000)) AS _q_0(_col_0)
LEFT JOIN _s5 AS _s5
  ON _s5.year_o_orderdate = column1
ORDER BY
  1 NULLS FIRST
