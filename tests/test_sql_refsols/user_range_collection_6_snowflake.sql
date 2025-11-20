WITH years AS (
  SELECT
    1990 + SEQ4() AS year
  FROM TABLE(GENERATOR(ROWCOUNT => 11))
), _s5 AS (
  SELECT
    YEAR(CAST(orders.o_orderdate AS TIMESTAMP)) AS year_oorderdate,
    COUNT(DISTINCT orders.o_custkey) AS ndistinct_ocustkey
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
  years.year,
  COALESCE(_s5.ndistinct_ocustkey, 0) AS n_orders
FROM years AS years
LEFT JOIN _s5 AS _s5
  ON _s5.year_oorderdate = years.year
ORDER BY
  1 NULLS FIRST
